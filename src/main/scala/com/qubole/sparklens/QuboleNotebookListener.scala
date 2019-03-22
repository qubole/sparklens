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

import com.qubole.sparklens.analyzer.{AppAnalyzer, EfficiencyStatisticsAnalyzer, ExecutorWallclockAnalyzer, StageSkewAnalyzer}
import com.qubole.sparklens.common.AppContext
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan}
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerTaskStart}
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by rohitk on 16/11/17.
  */

/*

Usage: python notebooks

1) Add this as first paragraph

QNL = sc._jvm.com.qubole.spyspark.QuboleNotebookListener.registerAndGet(sc._jsc.sc())
import time

def profileIt(callableCode, *args):
if (QNL.estimateSize() > QNL.getMaxDataSize()):
  QNL.purgeJobsAndStages()
startTime = long(round(time.time() * 1000))
result = callableCode(*args)
endTime = long(round(time.time() * 1000))
time.sleep(QNL.getWaiTimeInSeconds())
print(QNL.getStats(startTime, endTime))

2) wrap your code in some python function say myFunc
3) profileIt(myFunc)

Usage: scala notebooks

1) Add this as first paragraph

import com.qubole.spyspark.QuboleNotebookListener
val QNL = new QuboleNotebookListener(sc.getConf)
sc.addSparkListener(QNL)

2) Anywhere you need to profile the code:

QNL.profileIt {
    //Your code here
}


Usage: SQL paragraph

This doesn't work with SQL.
Use spark.sql(""" Your Query """) scala or python snippet to profile it

 */

class QuboleNotebookListener(sparkConf: SparkConf) extends QuboleJobListener(sparkConf: SparkConf) {

  private lazy val defaultExecutorCores = sparkConf.getInt("spark.executor.cores", 2)
  /**
    * No need to print statistics at the end of the Job
    * @param applicationEnd
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    stageMap.map(x => x._2).foreach(x => x.tempTaskTimes.clear())
    appInfo.endTime = applicationEnd.time
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val taskInfo = taskStart.taskInfo
    val executorTimeSpan = executorMap.get(taskInfo.executorId)
    if (!executorTimeSpan.isDefined) {
      val timeSpan = new ExecutorTimeSpan(taskInfo.executorId,
        taskInfo.host, defaultExecutorCores)
      timeSpan.setStartTime(taskInfo.launchTime)
      executorMap(taskInfo.executorId) = timeSpan
    }
    val hostTimeSpan = hostMap.get(taskInfo.host)
    if (!hostTimeSpan.isDefined) {
      val timeSpan = new HostTimeSpan(taskInfo.host)
      timeSpan.setStartTime(taskInfo.launchTime)
      hostMap(taskInfo.host) = timeSpan
    }
  }


  def estimateSize(): Long = {
    SizeEstimator.estimate(this)
  }

  def purgeJobsAndStages():Unit = {
    stageMap.clear()
    jobMap.clear()
  }

  def getStats(fromTime: Long, toTime: Long): String = {
    val list = new ListBuffer[AppAnalyzer]
    list += new StageSkewAnalyzer
    list += new ExecutorWallclockAnalyzer
    list += new EfficiencyStatisticsAnalyzer

    val appContext = new AppContext(appInfo,
      appMetrics,
      hostMap,
      executorMap,
      jobMap,
      jobSQLExecIDMap,
      stageMap,
      stageIDToJobID)

    val out = new mutable.StringBuilder()

    list.foreach(x => {
      try {
        val result = x.analyze(appContext, fromTime, toTime)
        out.append(result)
      } catch {
        case e:Throwable => {
          println(s"Failed in Analyzer ${x.getClass.getSimpleName}")
          e.printStackTrace()
        }
      }
    })
    out.toString()
  }

  def getMaxDataSize():Int  = maxDataSize
  def getWaiTimeInSeconds(): Int  = waitTimeMs/1000

  val maxDataSize = 32 * 1024 * 1024
  val waitTimeMs = 5000

  def profileIt[R](block: => R): R = {
    if (estimateSize() > maxDataSize ) {
      purgeJobsAndStages()
    }
    val startTime = System.currentTimeMillis()
    val result = block
    val endTime = System.currentTimeMillis()
    Thread.sleep(waitTimeMs)
    println(getStats(startTime,endTime))
    result
  }
}

object QuboleNotebookListener {
  def registerAndGet(sc: SparkContext): QuboleNotebookListener = {
    val listener = new QuboleNotebookListener(sc.getConf)
    sc.addSparkListener(listener)
    listener
  }
}
