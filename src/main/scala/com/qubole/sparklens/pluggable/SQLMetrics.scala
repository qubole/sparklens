package com.qubole.sparklens.pluggable

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.{InputAdapter, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.json4s.JsonAST.JValue
import org.json4s.DefaultFormats

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// ToDo: Remove the dependence on the SparkConf
// ToDo: Handle the cases in the offline reporting when most of the instance vals will be empty
// as only skewJoinInfo and stageToNode is getting dumped into the sparklens json.
class SQLMetrics(sparkConf: SparkConf) extends ComplimentaryMetrics {

  val stageToRDDIds = new mutable.HashMap[Int, List[Int]]()
  val stageToDuration = new mutable.HashMap[Int, Long]()
  val stageToTaskDurations = new mutable.HashMap[Int, ListBuffer[Long]]()
  var stageToNodes = new mutable.HashMap[Int, String]()
  var nodesDepthStrings: List[String] = _
  var skewJoinInfo: List[(String, Int)] = _


  def getRddId(plan: SparkPlan): Option[Int] = {
    try {
      val rddInfo = plan.getClass.getDeclaredField("rddID")
      rddInfo.get(plan).asInstanceOf[Option[Int]]
    } catch {
      case e: NoSuchFieldException =>
        // ToDO: Make available an open source version of the spark with the RDD IDs collection change
        println("RDD ids are not collected during execution, try using this distribution of spark: ")
        throw e
    }
  }

  def getJoinKeys(plan: SparkPlan): String = plan match {
    case x: ShuffledHashJoinExec =>
      (x.leftKeys, x.rightKeys).toString()
    case x: SortMergeJoinExec =>
      (x.leftKeys, x.rightKeys).toString()
    case x: SparkPlan if (getRddId(x).isDefined) =>
      x.children.map(getJoinKeys).toList.headOption.getOrElse("")
    case _ =>
      ""
  }

  /**
    * Extract output RDD ID and the join keys for the ShuffledHash and SortMerge joins in the sparkPlan
    */
  def extractJoinInfo(plan: SparkPlan): List[(String, Int)] = plan match {
    case x: SparkPlan if (getRddId(x).isDefined) =>
      // Case when the executed node is WholeStageCodegenExec or WholeStageCodegenExec, start
      // searching for the Join node from the child
      val subPlan = plan match {
        case _: WholeStageCodegenExec | _: InputAdapter =>
          plan.children.head
        case _ =>
          plan
      }
      val joinKeys = getJoinKeys(subPlan)
      if (!joinKeys.isEmpty) {
        (joinKeys, getRddId(x).get) :: x.children.map(extractJoinInfo).flatten.toList
      } else {
        x.children.map(extractJoinInfo).flatten.toList
      }
    case _ =>
      plan.children.map(extractJoinInfo).flatten.toList
  }

  /**
    * Returns stage ID in which this RDD was involved.
    */
  def getStageID(rddID: Int): Option[Int] = {
    stageToRDDIds.collectFirst {
      case (stageID: Int, stageRddIds: List[Int])
          if (stageRddIds.contains(rddID)) =>
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
    * Adds the stage ID - Node string pair to stageToNodes map
    */
  def populateNodeInStage(stageID: Int, nodeStr: String): Unit = {
    val nodeDepthStr = getNodeDepthString(nodeStr)
    if (stageToNodes.contains(stageID)) {
      stageToNodes(stageID) += s"\n${nodeDepthStr}"
    } else {
      stageToNodes(stageID) = nodeDepthStr
    }
  }

  /**
    * Maps which stage each node was executed as a part of
    */
  def mapNodesToStages(plan: SparkPlan, prevStageId: Int): Unit = {
    plan match {
      case node: SparkPlan =>
        val stageID = if (getRddId(node).isDefined) {
          getStageID(getRddId(node).get).getOrElse(-2)
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
    * Returns IDs of the RDDs involved in the skewed stage. Currently we are assuming that only one of the
    * stage might have the skewed task. ToDo: Fix this for multiple skewed stages application
    */
  def getSkewedStagesRddIds(): List[Int] = {

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
    stageIDs.map(stageToRDDIds(_)).flatten
  }

  def extractSkewJoinInfo(plan: SparkPlan): Unit = {
    val joinInfo = extractJoinInfo(plan)
    val skewedStagesRDDInfo = getSkewedStagesRddIds()
    skewJoinInfo = joinInfo.filter {
      case (_, rddId: Int) =>
        skewedStagesRDDInfo.exists(x => x.equals(rddId))
    }
  }

  def clearInfo(): Unit = {
    stageToDuration.clear()
    stageToTaskDurations.clear()
    stageToRDDIds.clear()
    stageToNodes.clear()
  }

  override def getMap(): Map[String, _] = {
    implicit val formats = DefaultFormats
    Map("stageToNodes" -> stageToNodes, "skewJoinInfo" -> skewJoinInfo)
  }
}

object SQLMetrics extends ComplimentaryMetrics {
  override def getObject(json: JValue): ComplimentaryMetrics = {
    implicit val formats = DefaultFormats
    // ToDo: Remove the dependence on the SparkConf
    val complimentaryMetrics = new SQLMetrics(new SparkConf())
    complimentaryMetrics.stageToNodes = (json \ "stageToNodes").extract[mutable.HashMap[Int, String]]
    complimentaryMetrics.skewJoinInfo ++ (json \ "skewJoinInfo").extract[mutable.HashMap[Int, String]]
    null
  }
}

