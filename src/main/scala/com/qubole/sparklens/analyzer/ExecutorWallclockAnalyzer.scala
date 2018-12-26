
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
package com.qubole.sparklens.analyzer

import java.util.concurrent.TimeUnit

import com.qubole.sparklens.scheduler.CompletionEstimator
import com.qubole.sparklens.common.{AggregateMetrics, AppContext}

import scala.collection.mutable

/*
 * Created by rohitk on 21/09/17.
 */
class ExecutorWallclockAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()

    val coresPerExecutor    =  AppContext.getExecutorCores(ac)
    val appExecutorCount    =  AppContext.getMaxConcurrent(ac.executorMap, ac).toInt
    val testPercentages     =  Array(10, 20, 50, 80, 100, 110, 120, 150, 200, 300, 400, 500)

    out.println ("\n App completion time and cluster utilization estimates with different executor counts")
    val appRealDuration = endTime - startTime
    printModelError(ac, appRealDuration, out)


    val pool = java.util.concurrent.Executors.newFixedThreadPool(testPercentages.size)
    val results = new mutable.HashMap[Int, String]()
    for (percent <- testPercentages) {
      pool.execute( new Runnable {
        override def run(): Unit = {
          val executorCount = (appExecutorCount * percent)/100
          if (executorCount > 0) {
            val estimatedTime = CompletionEstimator.estimateAppWallClockTime(ac, executorCount, coresPerExecutor, appRealDuration)
            val utilization =
              ac.stageMap.filter(x => x._2.stageMetrics.map.isDefinedAt(AggregateMetrics.executorRuntime))
                .map(x => x._2.stageMetrics.map(AggregateMetrics.executorRuntime).value).sum.toDouble*100/(estimatedTime*executorCount*coresPerExecutor)
            results.synchronized {
              results(percent) = f" Executor count ${executorCount}%5s  ($percent%3s%%) estimated time ${pd(estimatedTime)} and estimated cluster utilization ${utilization}%3.2f%%"
            }
          }
        }
      })
    }
    pool.shutdown()
    if (!pool.awaitTermination(2, TimeUnit.MINUTES)) {
      //we timed out
      out.println (
        s"""
           |WARN: Timed out calculating estimations for various executor counts.
           |WARN: ${results.size} of total ${testPercentages.size} estimates available at this time.
           |WARN: Please share the event log file with Qubole, to help us debug this further.
           |WARN: Apologies for the inconvenience.\n
         """.stripMargin)

    }
    //take a lock to prevent any conflicts while we are printing
    results.synchronized {
      results.toBuffer.sortWith((a, b) => a._1 < b._1)
        .foreach(x => {
          out.println(x._2)
        })
    }
    out.println("\n")
    out.toString()
  }

  def printModelError(ac: AppContext, appRealDuration: Long, out: mutable.StringBuilder): Unit = {
    val appExecutorCount = AppContext.getMaxConcurrent(ac.executorMap, ac).toInt
    val coresPerExecutor = AppContext.getExecutorCores(ac)

    @volatile var estimatedTime: Long = -1
    val thread = new Thread {
      override def run(): Unit = {
        estimatedTime = CompletionEstimator.estimateAppWallClockTime(ac, appExecutorCount, coresPerExecutor, appRealDuration)
      }
    }
    thread.setDaemon(true)
    thread.start()
    thread.join(60*1000)

    if (estimatedTime < 0) {
      //we timed out
      out.println (
        s"""
           |WARN: Timed out calculating model estimation time.
           |WARN: Please share the event log file with Qubole, to help us debug this further.
           |WARN: Apologies for the inconvenience.
         """.stripMargin)
      return
    }

    out.println (
      s"""
         | Real App Duration ${pd(appRealDuration)}
         | Model Estimation  ${pd(estimatedTime)}
         | Model Error       ${(Math.abs(appRealDuration-estimatedTime)*100)/appRealDuration}%
         |
         | NOTE: 1) Model error could be large when auto-scaling is enabled.
         |       2) Model doesn't handles multiple jobs run via thread-pool. For better insights into
         |          application scalability, please try such jobs one by one without thread-pool.
         |
       """.stripMargin)
  }
}
