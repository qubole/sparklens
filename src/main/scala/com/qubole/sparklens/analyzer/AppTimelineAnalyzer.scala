
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

import com.qubole.sparklens.common.{AggregateMetrics, AppContext}
import com.qubole.sparklens.timespan.JobTimeSpan

import scala.collection.mutable

/*
 * Prints information about all the jobs and shows how the
 * stages where scheduled within each job.
 */
class AppTimelineAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()
    out.println("\nPrinting Application timeline \n")
    val jobids = ac.jobMap.keySet.toBuffer.sortWith( _ < _ )
    out.append(s"${pt(startTime)} app started \n")
    jobids.map( x => (x, ac.jobMap(x)))
    .foreach( x => {
      val (jobID, jobTimeSpan) = x
      if (jobTimeSpan.duration().isDefined) {
        out.println(s"${pt(jobTimeSpan.startTime)} JOB ${jobID} started : duration ${pd(jobTimeSpan.duration().get)} ")
        printStageTimeLine(out, jobTimeSpan)
        val stageids = jobTimeSpan.stageMap.keySet.toBuffer.sortWith(_ < _)
        stageids.foreach(stageID => {
          val stageTimeSpan = jobTimeSpan.stageMap(stageID)
          val maxTaskTime = stageTimeSpan.stageMetrics.map(AggregateMetrics.executorRuntime).max
          if (stageTimeSpan.duration().isDefined) {
            out.println(s"${pt(stageTimeSpan.startTime)}      Stage ${stageID} started : duration ${pd(stageTimeSpan.duration().get)} ")
            out.println(s"${pt(stageTimeSpan.endTime)}      Stage ${stageID} ended : maxTaskTime ${maxTaskTime} taskCount ${stageTimeSpan.taskExecutionTimes.length}")
          }else {
            out.println(s"${pt(stageTimeSpan.startTime)}      Stage ${stageID} - duration not available ")
          }
        })
        out.println(s"${pt(jobTimeSpan.endTime)} JOB ${jobID} ended ")
      }else {
        out.println(s"${pt(jobTimeSpan.startTime)} JOB ${jobID} - duration not availble")
      }
    })
    out.println(s"${pt(endTime)} app ended \n")
    out.toString()
  }


  def printStageTimeLine(out: mutable.StringBuilder, jobTimeSpan: JobTimeSpan): Unit = {
    if (!jobTimeSpan.isFinished()) {
      return
    }
    val startTime = jobTimeSpan.startTime
    val endTime = jobTimeSpan.endTime
    val unit = {
      val x = (endTime-startTime)
      if (x <= 80) {
        1
      }else {
        x/80.toDouble
      }
    }

    jobTimeSpan.stageMap.filter(x => x._2.isFinished())
      .map(x => (x._1,
                (x._2.startTime-startTime)/unit,     //start position
                (x._2.endTime - startTime)/unit))    //end position
        .toBuffer.sortWith( (a, b) => a._1 < b._1)
          .foreach( x => {
            val (stageID, start, end) = x
            out.print(f"[${stageID}%7s ")
            out.print(" " * start.toInt)
            out.print("|" * (end.toInt - start.toInt))
            if (80 > end) {
              out.print(" " * (80 - end.toInt))
            }
            out.println("]")
          })
  }
}
