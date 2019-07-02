package org.apache.spark.sql

import com.qubole.sparklens.QuboleJobListener
import com.qubole.sparklens.pluggable.SQLMetrics
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListenerApplicationStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable.ListBuffer

class QuboleSQLListener(sparkConf: SparkConf) extends QuboleJobListener(sparkConf)
  with QueryExecutionListener {

  private var sqlMetrics: SQLMetrics = _

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    sqlMetrics = new SQLMetrics(sparkConf)
    pluggableMetricsMap("sqlMetrics") = sqlMetrics
    super.onApplicationStart(applicationStart)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    sqlMetrics.stageToRDDInfo(stageSubmitted.stageInfo.stageId) = stageSubmitted.stageInfo.rddInfos
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (!sqlMetrics.stageToTaskDurations.contains(taskEnd.stageId)) {
      sqlMetrics.stageToTaskDurations(taskEnd.stageId) = ListBuffer.empty
    }
    sqlMetrics.stageToTaskDurations(taskEnd.stageId) += taskEnd.taskInfo.duration
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    sqlMetrics.stageToRDDInfo(stageCompleted.stageInfo.stageId) = stageCompleted.stageInfo.rddInfos
    sqlMetrics.stageToDuration(stageCompleted.stageInfo.stageId) =
      stageCompleted.stageInfo.completionTime.getOrElse(0).asInstanceOf[Long] -
        stageCompleted.stageInfo.submissionTime.getOrElse(0).asInstanceOf[Long]
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    sqlMetrics.nodesDepthStrings = qe.executedPlan.toString.split("\n").toList
    sqlMetrics.extractSkewJoinInfo(qe.executedPlan)
    sqlMetrics.mapNodesToStages(qe.executedPlan, -1)
    sqlMetrics.clearInfo()
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    sqlMetrics.clearInfo()
  }
}
