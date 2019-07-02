package com.qubole.sparklens.pluggable
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{InputAdapter, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.storage.RDDInfo
import org.json4s.JsonAST.JValue
import org.json4s.MonadicJValue
import org.json4s.DefaultFormats

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// ToDo: Remove the dependence on the SparkConf
// ToDo: Handle the cases in the offline reporting when most of the instance vals will be empty
// as only skewJoinInfo and stageToNode is getting dumped into the sparklens json.
class SQLMetrics(sparkConf: SparkConf) extends ComplimentaryMetrics {

  val stageToRDDInfo = new mutable.HashMap[Int, Seq[RDDInfo]]()
  val stageToDuration = new mutable.HashMap[Int, Long]()
  val stageToTaskDurations = new mutable.HashMap[Int, ListBuffer[Long]]()
  var stageToNode = new mutable.HashMap[Int, String]()
  var nodesDepthStrings: List[String] = _
  var skewJoinInfo: List[((Seq[Expression], Seq[Expression]), RDD[InternalRow])] = _

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
    * Extract output RDD and the join keys for the ShuffledHash and SortMerge joins in the sparkPlan
    */
  def extractJoinInfo(plan: SparkPlan):
  List[((Seq[Expression], Seq[Expression]), RDD[InternalRow])] = plan match {
    case x: SparkPlan if (getRDD(x) != null) =>
      // Case when the executed node is WholeStageCodegenExec or WholeStageCodegenExec, start
      // searching for the Join node from the child
      val subPlan = plan match {
        case _: WholeStageCodegenExec | _: InputAdapter =>
          plan.children.head
        case _ =>
          plan
      }
      val joinKeys = getJoinKeys(subPlan)
      if (!joinKeys._1.isEmpty) {
        (joinKeys.to, getRDD(x)) :: x.children.map(extractJoinInfo).flatten.toList
      } else {
        x.children.map(extractJoinInfo).flatten.toList
      }
    case _ =>
      plan.children.map(extractJoinInfo).flatten.toList
  }

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

  /**
    * Returns stage ID in which this RDD was involved.
    * @param rddID
    * @return
    */
  def getStageID(rddID: Int): Option[Int] = {
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
          getStageID(getRDD(node).id).getOrElse(-1)
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

  def isGreaterPercentage(individual: Long, aggregate: Long, threshold: Double): Boolean = {
    return individual.asInstanceOf[Double] / aggregate >= threshold
  }

  /**
    * Returns if the given stage has any skewed task
    * ToDo: Add a combination of the condition on the time as well as shuffle read/write
    */
  def containsSkewedTask(stageID: Int): Boolean = {
    val totalTaskDuration = stageToTaskDurations(stageID).sum
    // Should we have a single task condition?
    stageToTaskDurations(stageID).size > 1 &&
      stageToTaskDurations(stageID).filter {
        case taskDuration: Long =>
          isGreaterPercentage(taskDuration, totalTaskDuration,
            sparkConf.getDouble("spark.skew.taskTimeThreshold", 0.001))
      }.nonEmpty
  }

  /**
    * Returns RDDs involved in the skewed stage. Currently we are assuming that only one of the
    * stage might have the skewed task. ToDo: Fix this for multiple skewed stages application
    */
  def getSkewedStagesRDDInfo(): List[RDDInfo] = {

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
    stageIDs.map(stageToRDDInfo).flatten
  }

  def extractSkewJoinInfo(plan: SparkPlan): Unit = {
    val joinInfo = extractJoinInfo(plan)
    val skewedStagesRDDInfo = getSkewedStagesRDDInfo()
    skewJoinInfo = joinInfo.filter {
      case ((_, _), rdd: RDD[InternalRow]) =>
        skewedStagesRDDInfo.exists(x => x.id.equals(rdd.id))
    }
  }


  def clearInfo(): Unit = {
    stageToDuration.clear()
    stageToTaskDurations.clear()
    stageToRDDInfo.clear()
    stageToNode.clear()
  }

  override def getMap(): Map[String, _] = {
    implicit val formats = DefaultFormats
    Map("stageToNode" -> stageToNode, "skewJoinInfo" -> skewJoinInfo)
  }
}

object SQLMetrics extends ComplimentaryMetrics {
  override def getObject(json: JValue): ComplimentaryMetrics = {
    implicit val formats = DefaultFormats
    // ToDo: Remove the dependence on the SparkConf
    val complimentaryMetrics = new SQLMetrics(new SparkConf())
    complimentaryMetrics.stageToNode = (json \ "stageToNode").extract[mutable.HashMap[Int, String]]
    complimentaryMetrics.skewJoinInfo ++ (json \ "skewJoinInfo").extract[mutable.HashMap[Int, String]]
  } 
}

