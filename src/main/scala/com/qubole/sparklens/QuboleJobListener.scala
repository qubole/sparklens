/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.qubole.sparklens

import java.net.URI

import com.qubole.sparklens.analyzer._
import com.qubole.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by rohitk on 21/09/17.
  *
  *
  */

class QuboleJobListener(sparkConf: SparkConf)  extends SparkListener {

  protected val appInfo          = new ApplicationInfo()
  protected val executorMap      = new mutable.HashMap[String, ExecutorTimeSpan]()
  protected val hostMap          = new mutable.HashMap[String, HostTimeSpan]()
  protected val jobMap           = new mutable.HashMap[Long, JobTimeSpan]
  protected val stageMap         = new mutable.HashMap[Int, StageTimeSpan]
  protected val stageIDToJobID   = new mutable.HashMap[Int, Long]
  protected val failedStages     = new ListBuffer[String]
  protected val appMetrics       = new AggregateMetrics()

  private def hostCount():Int = hostMap.size

  private def executorCount(): Int = executorMap.size

  private def coresPerExecutor(): Int = {
    // just for the fun
    executorMap.values.map(x => x.cores).sum/executorMap.size
  }
  private def executorsForHost(hostID: String) : Seq[String] = {
    executorMap.values.filter(_.hostID.equals(hostID)).map(x => x.executorID).toSeq
  }
  private def executorsPerHost(): Double = {
    executorCount()/hostCount()
  }
  private def appAggregateMetrics(): AggregateMetrics = appMetrics

  private def hostAggregateMetrics(hostID: String): Option[AggregateMetrics] = {
    hostMap.get(hostID).map(x => x.hostMetrics)
  }

  private def executorAggregateMetrics(executorID: String): Option[AggregateMetrics] = {
    executorMap.get(executorID).map(x => x.executorMetrics)
  }

  private def driverTimePercentage(): Option[Double] = {
    if (appInfo.endTime == 0) {
      return None
    }
    val totalTime = appInfo.endTime - appInfo.startTime
    val jobTime   = jobMap.values.map(x => (x.endTime - x.startTime)).sum
    Some((totalTime-jobTime)/totalTime)
  }

  private def jobTimePercentage(): Option[Double] = {
    if (appInfo.endTime == 0) {
      return None
    }
    val totalTime = appInfo.endTime - appInfo.startTime
    val jobTime   = jobMap.values.map(x => (x.endTime - x.startTime)).sum
    Some(jobTime/totalTime)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskMetrics = taskEnd.taskMetrics
    val taskInfo    = taskEnd.taskInfo

    if (taskMetrics == null) return
    
    //update app metrics
    appMetrics.update(taskMetrics, taskInfo)
    val executorTimeSpan = executorMap.get(taskInfo.executorId)
    if (executorTimeSpan.isDefined) {
      //update the executor metrics
      executorTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
    }
    val hostTimeSpan = hostMap.get(taskInfo.host)
    if (hostTimeSpan.isDefined) {
      //also update the host metrics
      hostTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
    }

    val stageTimeSpan = stageMap.get(taskEnd.stageId)
    if (stageTimeSpan.isDefined) {
      //update stage metrics
      stageTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
      stageTimeSpan.get.updateTasks(taskInfo, taskMetrics)
    }
    val jobID = stageIDToJobID.get(taskEnd.stageId)
    if (jobID.isDefined) {
      val jobTimeSpan = jobMap.get(jobID.get)
      if (jobTimeSpan.isDefined) {
        //update job metrics
        jobTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
      }
    }

    if (taskEnd.taskInfo.failed) {
      //println(s"\nTask Failed \n ${taskEnd.reason}"
    }
  }

  private[this] def dumpData(appContext: AppContext): Unit = {
    val dumpDir = getDumpDirectory(sparkConf)
    println(s"Saving sparkLens data to ${dumpDir}")
    val fs = FileSystem.get(new URI(dumpDir), new Configuration())
    val stream = fs.create(new Path(s"${dumpDir}/${appInfo.applicationID}.sparklens.json"))
    val jsonString = appContext.toString
    stream.writeBytes(jsonString)
    stream.close()
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    //println(s"Application ${applicationStart.appId} started at ${applicationStart.time}")
    appInfo.applicationID = applicationStart.appId.getOrElse("NA")
    appInfo.startTime     = applicationStart.time
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    stageMap.map(x => x._2).foreach( x => x.tempTaskTimes.clear())
    //println(s"Application ${appInfo.applicationID} ended at ${applicationEnd.time}")
    appInfo.endTime = applicationEnd.time

    val appContext = new AppContext(appInfo,
      appMetrics,
      hostMap,
      executorMap,
      jobMap,
      stageMap,
      stageIDToJobID)

    asyncReportingEnabled(sparkConf) match {
      case true => {
        println("Reporting disabled. Will save sparklens data file for later use.")
        dumpData(appContext)
      }
      case false => {
        if (dumpDataEnabled(sparkConf)) dumpData(appContext)
        AppAnalyzer.startAnalyzers(appContext)
      }
    }
  }
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val executorTimeSpan = executorMap.get(executorAdded.executorId)
    if (!executorTimeSpan.isDefined) {
      val timeSpan = new ExecutorTimeSpan(executorAdded.executorId,
        executorAdded.executorInfo.executorHost,
        executorAdded.executorInfo.totalCores)
      timeSpan.setStartTime(executorAdded.time)
       executorMap(executorAdded.executorId) = timeSpan
    }
    val hostTimeSpan = hostMap.get(executorAdded.executorInfo.executorHost)
    if (!hostTimeSpan.isDefined) {
      val executorHostTimeSpan = new HostTimeSpan(executorAdded.executorInfo.executorHost)
      executorHostTimeSpan.setStartTime(executorAdded.time)
      hostMap(executorAdded.executorInfo.executorHost) = executorHostTimeSpan
    }
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    val executorTimeSpan = executorMap(executorRemoved.executorId)
    executorTimeSpan.setEndTime(executorRemoved.time)
    //We don't get any event for host. Will not try to check when the hosts go out of service
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val jobTimeSpan = new JobTimeSpan(jobStart.jobId)
    jobTimeSpan.setStartTime(jobStart.time)
    jobMap(jobStart.jobId) = jobTimeSpan
    jobStart.stageIds.foreach( stageID => {
      stageIDToJobID(stageID) = jobStart.jobId
    })
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobTimeSpan = jobMap(jobEnd.jobId)
    jobTimeSpan.setEndTime(jobEnd.time)
    //if we miss cleaing up tasks at end of stage, clean them after end of job
    stageMap.map(x => x._2).foreach( x => x.tempTaskTimes.clear())
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    if (!stageMap.get(stageSubmitted.stageInfo.stageId).isDefined) {
      val stageTimeSpan = new StageTimeSpan(stageSubmitted.stageInfo.stageId,
        stageSubmitted.stageInfo.numTasks)
      stageTimeSpan.setParentStageIDs(stageSubmitted.stageInfo.parentIds)
      if (stageSubmitted.stageInfo.submissionTime.isDefined) {
        stageTimeSpan.setStartTime(stageSubmitted.stageInfo.submissionTime.get)
      }
      stageMap(stageSubmitted.stageInfo.stageId) = stageTimeSpan
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageTimeSpan = stageMap(stageCompleted.stageInfo.stageId)
    if (stageCompleted.stageInfo.completionTime.isDefined) {
      stageTimeSpan.setEndTime(stageCompleted.stageInfo.completionTime.get)
    }
    if (stageCompleted.stageInfo.submissionTime.isDefined) {
      stageTimeSpan.setStartTime(stageCompleted.stageInfo.submissionTime.get)
    }

    if (stageCompleted.stageInfo.failureReason.isDefined) {
      //stage failed
      val si = stageCompleted.stageInfo
      failedStages += s""" Stage ${si.stageId} attempt ${si.attemptId} in job ${stageIDToJobID(si.stageId)} failed.
                      Stage tasks: ${si.numTasks}
                      """
      stageTimeSpan.finalUpdate()
    }else {
      val jobID = stageIDToJobID(stageCompleted.stageInfo.stageId)
      val jobTimeSpan = jobMap(jobID)
      jobTimeSpan.addStage(stageTimeSpan)
      stageTimeSpan.finalUpdate()
    }
    stageTimeSpan.tempTaskTimes.clear()
  }
}