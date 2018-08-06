
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

import java.util.Locale
import com.qubole.sparklens.common.{AggregateMetrics, AppContext}
import scala.collection.mutable

/*
 * Created by rohitk on 21/09/17.
 */
class StageSkewAnalyzer extends  AppAnalyzer {


  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()
    computePerStageEfficiencyStatistics(ac, out)
    checkForGCOrShuffleService(ac, out)
    out.toString()
  }


  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (Math.abs(size) >= 1*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (Math.abs(size) >= 1*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (Math.abs(size) >= 1*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else {
        (size.asInstanceOf[Double] / KB, "KB")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  def computePerStageEfficiencyStatistics(ac: AppContext, out: mutable.StringBuilder): Unit = {

    val totalTasks = ac.stageMap.map(x => x._2.taskExecutionTimes.length).sum
    out.println (s"Total tasks in all stages ${totalTasks}")

    // sum of cores in all the executors
    // we are assuming no autoscaling here
    out.println ("Per Stage  Utilization")
    out.println (s"Stage-ID   Wall    Task      Task     IO%    Input     Output    ----Shuffle-----    -WallClockTime-    --OneCoreComputeHours---   MaxTaskMem")
    out.println (s"          Clock%  Runtime%   Count                               Input  |  Output    Measured | Ideal   Available| Used%|Wasted%                                  ")

    val maxExecutors = AppContext.getMaxConcurrent(ac.executorMap, ac)
    val totalCores = ac.executorMap.values.last.cores * maxExecutors
    val totalMillis = ac.stageMap.map(x =>
        x._2.duration().getOrElse(0L)
    ).sum * totalCores

    val totalRuntime = ac.stageMap.map(x =>
                              x._2.stageMetrics.map(AggregateMetrics.executorRuntime).value).sum

    val totalExecutors = ac.executorMap.size
    val totalIOBytes   = ac.jobMap.values.map ( x => (  x.jobMetrics.map(AggregateMetrics.inputBytesRead).value
                                                      + x.jobMetrics.map(AggregateMetrics.outputBytesWritten).value
                                                      + x.jobMetrics.map(AggregateMetrics.shuffleWriteBytesWritten).value
                                                      + x.jobMetrics.map(AggregateMetrics.shuffleReadBytesRead).value)
                                              ).sum

    ac.stageMap.keySet
      .toBuffer
      .sortWith( _ < _ )
      .filter( x => ac.stageMap.get(x).get.endTime != 0)
      .foreach(x => {
        val sts = ac.stageMap.get(x).get
        val duration = sts.duration().get
        val available = totalCores * duration
        val stagePercent = (available*100/totalMillis.toFloat).toInt
        val used      = sts.stageMetrics.map(AggregateMetrics.executorRuntime).value
        val wasted    = available - used
        val usedPercent = (used * 100)/available.toFloat
        val wastedPercent = (wasted * 100)/available.toFloat
        val executorCores = totalCores/totalExecutors
        val stageBytes = sts.stageMetrics.map(AggregateMetrics.inputBytesRead).value
                       + sts.stageMetrics.map(AggregateMetrics.outputBytesWritten).value
                       + sts.stageMetrics.map(AggregateMetrics.shuffleWriteBytesWritten).value
                       + sts.stageMetrics.map(AggregateMetrics.shuffleReadBytesRead).value
        val maxTaskMemory = sts.taskPeakMemoryUsage.take(executorCores.toInt).sum // this could
        // be at different times?
      //val maxTaskMemoryUtilization = (maxTaskMemory*100)/executorMemory
        val IOPercent = (stageBytes* 100)/ totalIOBytes.toFloat
        val taskRuntimePercent = (sts.stageMetrics.map(AggregateMetrics.executorRuntime).value * 100)/totalRuntime.toFloat
        val idealWallClock = sts.stageMetrics.map(AggregateMetrics.executorRuntime).value/(totalExecutors * executorCores)

        out.println (f"${x}%8s   ${stagePercent}%5.2f   ${taskRuntimePercent}%5.2f   ${sts.taskExecutionTimes.length}%7s  " +
          f"${IOPercent}%5.1f  ${bytesToString(sts.stageMetrics.map(AggregateMetrics.inputBytesRead).value)}%8s " +
          f" ${bytesToString(sts.stageMetrics.map(AggregateMetrics.outputBytesWritten).value)}%8s  " +
          f"${bytesToString(sts.stageMetrics.map(AggregateMetrics.shuffleReadBytesRead).value)}%8s " +
          f" ${bytesToString(sts.stageMetrics.map(AggregateMetrics.shuffleWriteBytesWritten).value)}%8s    " +
          f"${pd(duration)}   ${pd(idealWallClock)} ${pcm(available)}%10s  $usedPercent%5.1f  $wastedPercent%5.1f  ${bytesToString(maxTaskMemory)}%8s ")
    })

    val maxMem = ac.stageMap.keySet.map(key => {
      ac.stageMap.get(key).get.taskPeakMemoryUsage.take((totalCores/totalExecutors).toInt).sum
    }).toSeq.sorted.last
    out.println(f"Max memory which an executor could have taken = ${bytesToString(maxMem)}%8s")

    out.println("\n")
  }

  def checkForGCOrShuffleService(ac: AppContext, out: mutable.StringBuilder): Unit = {
    val maxExecutors = AppContext.getMaxConcurrent(ac.executorMap, ac)
    val totalCores = ac.executorMap.values.last.cores * maxExecutors
    val totalMillis = ac.stageMap.filter(x => x._2.endTime > 0).map(x => x._2.duration().get).sum * totalCores
    out.println (s" Stage-ID WallClock  OneCore       Task   PRatio    -----Task------   OIRatio  |* ShuffleWrite% ReadFetch%   GC%  *|")
    out.println (s"          Stage%     ComputeHours  Count            Skew   StageSkew                                                ")

    ac.stageMap.keySet.toBuffer.sortWith( _ < _ )
      .filter( x => ac.stageMap(x).endTime > 0)
      .foreach(x => {
      val sts =  ac.stageMap(x)
      val totalExecutorTime     = sts.stageMetrics.map(AggregateMetrics.executorRuntime).value
      //shuffleWriteTime is in nanoSeconds
      val writeTimePercent:Float = (sts.stageMetrics.map(AggregateMetrics.shuffleWriteTime).value.toFloat * 100)/totalExecutorTime/(1000*1000)
      val readFetchPercent:Float  = (sts.stageMetrics.map(AggregateMetrics.shuffleReadFetchWaitTime).value.toFloat * 100)/ totalExecutorTime
      val gcPercent:Float        = (sts.stageMetrics.map(AggregateMetrics.jvmGCTime).value.toFloat * 100) / totalExecutorTime

      val available = totalCores * ac.stageMap.get(x).get.duration.get
      val stagePercent:Float = (available.toFloat*100/totalMillis)
      val parallelismRatio:Float  = sts.stageMetrics.count.toFloat/totalCores
      val maxTaskTime = sts.taskExecutionTimes.max
      val meanTaskTime = if (sts.taskExecutionTimes.length == 0) {
        0
      }else if (sts.taskExecutionTimes.length == 1) {
        sts.taskExecutionTimes(0)
      }else {
        sts.taskExecutionTimes.sortWith(_ < _ )(sts.taskExecutionTimes.length/2)
      }

      val taskSkew:Float  = if (meanTaskTime > 0) {
        maxTaskTime.toFloat / meanTaskTime
      }else {
        0
      }
        val duration = sts.duration().get
      val taskStageSkew: Float = if (duration > 0) {
        maxTaskTime.toFloat/duration
      } else {
        0
      }


      val totalInput = sts.stageMetrics.map(AggregateMetrics.inputBytesRead).value + sts.stageMetrics.map(AggregateMetrics.shuffleReadBytesRead).value
      val totalOutput = sts.stageMetrics.map(AggregateMetrics.outputBytesWritten).value+ sts.stageMetrics.map(AggregateMetrics.shuffleWriteBytesWritten).value
      val oiRatio:Float = if (totalInput == 0) {
        0
      }else {
        totalOutput.toFloat/totalInput
      }
      out.println (f"${x}%7s ${stagePercent}%7.2f   ${pcm(totalExecutorTime)}%13s ${sts.taskExecutionTimes.length}%7s $parallelismRatio%7.2f  $taskSkew%7.2f  $taskStageSkew%7.2f  $oiRatio%7.2f     |* ${writeTimePercent}%6.2f  ${readFetchPercent}%13.2f   ${gcPercent}%6.2f  *|")

    })

    out.println(
      """
        |PRatio:        Number of tasks in stage divided by number of cores. Represents degree of
        |               parallelism in the stage
        |TaskSkew:      Duration of largest task in stage divided by duration of median task.
        |               Represents degree of skew in the stage
        |TaskStageSkew: Duration of largest task in stage divided by total duration of the stage.
        |               Represents the impact of the largest task on stage time.
        |OIRatio:       Output to input ration. Total output of the stage (results + shuffle write)
        |               divided by total input (input data + shuffle read)
        |
        |These metrics below represent distribution of time within the stage
        |
        |ShuffleWrite:  Amount of time spent in shuffle writes across all tasks in the given
        |               stage as a percentage
        |ReadFetch:     Amount of time spent in shuffle read across all tasks in the given
        |               stage as a percentage
        |GC:            Amount of time spent in GC across all tasks in the given stage as a
        |               percentage
        |
        |If the stage contributes large percentage to overall application time, we could look into
        |these metrics to check which part (Shuffle write, read fetch or GC is responsible)
        |
        |
      """.stripMargin)
  }
}
