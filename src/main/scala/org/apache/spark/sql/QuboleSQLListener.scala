package org.apache.spark.sql

import com.qubole.sparklens.QuboleJobListener
import com.qubole.sparklens.pluggable.SQLMetrics
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListenerApplicationStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.storage.RDDInfo

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class QuboleSQLListener(sparkConf: SparkConf) extends QuboleJobListener(sparkConf)
  with QueryExecutionListener {

  private val stageToRDDInfo = new mutable.HashMap[Int, Seq[RDDInfo]]()
  private val stageToDuration = new mutable.HashMap[Int, Long]()
  private val stageToTaskDurations = new mutable.HashMap[Int, ListBuffer[Long]]()
  private val stageToNode = new mutable.HashMap[Int, String]()
  private var nodesDepthStrings: List[String] = _

  /**
    * Extracts the first matching join node from the spark plan
    */
  def getRDD(plan: SparkPlan): RDD[InternalRow] = {
    try {
      val rddInfo = plan.getClass.getDeclaredField("RDDInfo")
      rddInfo.get(plan).asInstanceOf[RDD[InternalRow]]
    } catch {
      case e: NoSuchFieldException =>
        println("RDDInfo is not collected, try using this distribution of spark: ")
        throw e
    }
  }

  def getJoinKeys(plan: SparkPlan): (Seq[Expression], Seq[Expression]) = plan match {
    case x: ShuffledHashJoinExec =>
      (x.leftKeys, x.rightKeys)
    case x: SortMergeJoinExec =>
      (x.leftKeys, x.rightKeys)
    case x: SparkPlan if (getRDD(x) == null) =>
      x.children.map(getJoinKeys).toList.headOption.getOrElse((Seq.empty, Seq.empty))
    case _ =>
      (Seq.empty, Seq.empty)
  }

  /**
    * Extracts all the join nodes (join keys) and the output RDD of those nodes
    * @param plan
    * @return
    */
  def extractJoinRDDInfo(plan: SparkPlan):
  List[((Seq[Expression], Seq[Expression]), RDD[InternalRow])] = plan match {
    case x: SparkPlan if (getRDD(x) != null) =>
      // Case when the executed node is WholeStageCodegenExec or WholeStageCodegenExec, start
      // searching for the Join node from the child
      val subPlan = plan match {
        case _: WholeStageCodegenExec | _: InputAdapter =>
          plan.asInstanceOf[UnaryExecNode].child
        case _ =>
          plan
      }
      val keys = getJoinKeys(subPlan)
      if (!keys._1.isEmpty) {
        (keys, getRDD(x)) :: x.children.map(extractJoinRDDInfo).flatten.toList
      } else {
        x.children.map(extractJoinRDDInfo).flatten.toList
      }
    case _ =>
      plan.children.map(extractJoinRDDInfo).flatten.toList
  }

  /**
    * Returns stage ID in which this RDD was involved.
    * @param rddID
    * @return
    */
  def getStageId(rddID: Int): Option[Int] = {
    stageToRDDInfo.collectFirst {
      case (stageID: Int, rddInfo: Seq[RDDInfo])
        if (rddInfo.map(_.id).contains(rddID)) =>
        stageID
    }
  }


  /**
    * Returns the depth formatted form of the node string
    */
  def getNodeDepthString(nodeString: String): String = {
    nodesDepthStrings.find {
      case str: String =>
        str.contains(nodeString)
    }.getOrElse(nodeString)
  }

  /**
    * Adds the stage ID - Node string pair to stageToNode map
    */
  def populateNodeInStage(stageID: Int, nodeStr: String): Unit = {
    val nodeDepthStr = getNodeDepthString(nodeStr)
    if (stageToNode.contains(stageID)) {
      stageToNode(stageID) += s"\n${nodeDepthStr}"
    } else {
      stageToNode(stageID) = nodeDepthStr
    }
  }

  /**
    * Maps which stage each node was executed as a part of
    */
  def mapNodesToStages(plan: SparkPlan, prevStageId: Int): Unit = {
    plan match {
      case node: SparkPlan =>
        val stageID = if (getRDD(node) != null) {
          getStageId(getRDD(node).id).getOrElse(-1)
        } else {
          prevStageId
        }
        if (!(node.isInstanceOf[WholeStageCodegenExec] || node.isInstanceOf[InputAdapter])) {
          populateNodeInStage(stageID, node.toString.split("\n").head)
        }
        node.children.map(mapNodesToStages(_, stageID))
      case _ =>
    }
  }

  def clearInformation(): Unit = {
    stageToDuration.clear()
    stageToTaskDurations.clear()
    stageToRDDInfo.clear()
    stageToNode.clear()
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    nodesDepthStrings = qe.executedPlan.toString.split("\n").toList
    val nodeToRDDInfo = extractJoinRDDInfo(qe.executedPlan)
    val skewedStagesRDD = getSkewedStagesRDD()
    val culpritJoin = nodeToRDDInfo.find {
      case ((_, _), rdd: RDD[InternalRow]) =>
        skewedStagesRDD.exists(x => x.id.equals(rdd.id))
    }
    mapNodesToStages(qe.executedPlan, -1)

    clearInformation()
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    clearInformation()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (!stageToTaskDurations.contains(taskEnd.stageId)) {
      stageToTaskDurations(taskEnd.stageId) = ListBuffer.empty
    }
    stageToTaskDurations(taskEnd.stageId) += taskEnd.taskInfo.duration
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stageToRDDInfo(stageSubmitted.stageInfo.stageId) = stageSubmitted.stageInfo.rddInfos
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageToRDDInfo(stageCompleted.stageInfo.stageId) = stageCompleted.stageInfo.rddInfos
    stageToDuration(stageCompleted.stageInfo.stageId) =
      stageCompleted.stageInfo.completionTime.getOrElse(0).asInstanceOf[Long] -
        stageCompleted.stageInfo.submissionTime.getOrElse(0).asInstanceOf[Long]
  }

  def isGreaterPercentage(individual: Long, aggregate: Long, threshold: Double): Boolean = {
    return individual.asInstanceOf[Double] / aggregate >= threshold
  }

  /**
    * Returns if the given stage has any skewed task
    * ToDo: Add a combination of the condition on the time as well as shuffle read/write
    */
  def containsSkewedTask(stageID: Int): Boolean = {
    val totalTaskDuration = stageToTaskDurations(stageID).sum
    stageToTaskDurations(stageID).size > 1 &&
      stageToTaskDurations(stageID).collect {
        case taskDuration: Long
          if isGreaterPercentage(taskDuration, totalTaskDuration,
            sparkConf.getDouble("spark.skew.taskTimeThreshold", 0.001)) =>
          taskDuration
      }.nonEmpty
  }

  /**
    * Returns RDDs involved in the skewed stage. Currently we are assuming that only one of the
    * stage might have the skewed task. ToDo: Fix this for multiple skewed stages application
    */
  def getSkewedStagesRDD(): List[RDDInfo] = {

    def getStageIds(): List[Int] = {
      try {
        val stageIds = sparkConf.get("spark.skew.stage.id").split(",")
        stageIds.map(x => x.toInt).toList
      } catch {
        case e: Exception =>
          List.empty
      }
    }

    val stageIDs = if (sparkConf.contains("spark.skew.stage.id")) {
      getStageIds()
    } else {
      val totalStageTime = stageToDuration.map(_._2).sum
      stageToDuration.collect{
        case (stageID: Int, stageDuration: Long)
          if (isGreaterPercentage(stageDuration, totalStageTime,
            sparkConf.getDouble("spark.shew.stageTimeThreshold", 0.0001))
            && containsSkewedTask(stageID)) =>
          stageID
      }.toList
    }
    stageIDs.map(x => stageToRDDInfo(x.asInstanceOf[Int])).flatten
  }


  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    pluggableMetricsMap("SQLInfo") = new SQLMetrics()
    super.onApplicationStart(applicationStart)
  }
}
