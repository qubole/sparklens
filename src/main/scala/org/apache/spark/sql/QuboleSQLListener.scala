package org.apache.spark.sql

import com.qubole.sparklens.QuboleJobListener
import com.qubole.sparklens.pluggable.SQLMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.{SparkListenerApplicationStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable.ListBuffer

class QuboleSQLListener(sparkConf: SparkConf, jobListener: QuboleJobListener) extends QueryExecutionListener {

  private val sqlMetrics: SQLMetrics = new SQLMetrics(sparkConf)

  def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    jobListener.pluggableMetricsMap("sqlMetrics") = sqlMetrics
  }

  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (!sqlMetrics.stageToTaskDurations.contains(taskEnd.stageId)) {
      sqlMetrics.stageToTaskDurations(taskEnd.stageId) = ListBuffer.empty
    }
    sqlMetrics.stageToTaskDurations(taskEnd.stageId) += taskEnd.taskInfo.duration
  }

  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    sqlMetrics.stageToRDDIds(stageCompleted.stageInfo.stageId) =
      stageCompleted.stageInfo.rddInfos.map(_.id).toList
    sqlMetrics.stageToDuration(stageCompleted.stageInfo.stageId) =
      stageCompleted.stageInfo.completionTime.getOrElse(0).asInstanceOf[Long] -
        stageCompleted.stageInfo.submissionTime.getOrElse(0).asInstanceOf[Long]
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    sqlMetrics.nodesDepthStrings = qe.executedPlan.toString.split("\n").toList
    sqlMetrics.extractSkewJoinInfo(qe.executedPlan)
    sqlMetrics.mapNodesToStages(qe.executedPlan, -1)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    sqlMetrics.clearInfo()
  }
}
