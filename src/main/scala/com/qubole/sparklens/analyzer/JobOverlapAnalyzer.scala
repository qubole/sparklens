
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

import com.qubole.sparklens.common.AppContext
import com.qubole.sparklens.timespan.JobTimeSpan

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/*
 * Created by rohitk on 21/09/17.
 */
class JobOverlapAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val printDetailedReport = true
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()
    out.println ("\nChecking for job overlap...\n")

    var lastEndTime:Long  = 0
    val jobsList = JobOverlapAnalyzer.makeJobLists(ac)
    var count = 1
    val conflictingJobGroups = new mutable.ListBuffer[( Long, Long)]

    jobsList.sortWith( (a, b) => a.map( x => x.startTime).min < b.map( x => x.startTime).min )
      .foreach( x => {
        val minStartTime = x.map( x => x.startTime).min
        val maxEndTime   = x.map( x => x.endTime).max
        val jobIDList = x.map(x => x.jobID)
        val groupID = {
          if (ac.jobSQLExecIdMap.contains(jobIDList.last)) {
            ac.jobSQLExecIdMap(jobIDList.last)
          }else {
            -1
          }
        }
        if (printDetailedReport) {
          out.println(" ")
          out.println(s" JobGroup ${count}  SQLExecID (${groupID})")
          out.println(s" Number of Jobs ${jobIDList.size}  JobIDs(${jobIDList.mkString(",")})")
          out.println(s" Timing [${pt(minStartTime)} - ${pt(maxEndTime)}]")
          out.println(s" Duration  ${pd(maxEndTime - minStartTime)}")
          out.println(" ")

          if (jobIDList.size > 1) {
            printJobGroupTimeLine(out, x)
          }
          x.sortWith((a, b) => a.jobID < b.jobID).foreach(j => {
            out.println(s" JOB ${j.jobID} Start ${pt(j.startTime)}  End ${pt(j.endTime)}")
          })
          out.println(" ")
        }

        if (lastEndTime != 0) {
          if (lastEndTime > minStartTime) {
            conflictingJobGroups += ((count, count-1))
          }
        }
        lastEndTime = maxEndTime
        count += 1
    })

    if (conflictingJobGroups.size > 0) {
      out.println(s"Found ${conflictingJobGroups.size} overlapping JobGroups. Using threadpool for submitting parallel jobs? " +
        s"Some calculations might not be reliable.")
      conflictingJobGroups.foreach( x => {
        out.println(s"Running with overlap:  JobGroupID ${x._1} && JobGroupID ${x._2} ")
      })
    }else {
      out.println ("\nNo overlapping jobgroups found. Good\n")
    }
    out.println("\n")
    out.toString()
  }

  def printJobGroupTimeLine(out: mutable.StringBuilder, listJobTimeSpan: List[JobTimeSpan]): Unit = {

    val startTime = listJobTimeSpan.map( x => x.startTime).min
    val endTime = listJobTimeSpan.map(x => x.endTime).max
    val unit = {
      val x = (endTime-startTime)
      if (x <= 80) {
        1
      }else {
        x/80.toDouble
      }
    }

    listJobTimeSpan.filter(x => x.isFinished())
      .map(x => (x.jobID,
        (x.startTime-startTime)/unit,     //start position
        (x.endTime - startTime)/unit,
        x.stageMap.map(x => x._2.taskExecutionTimes.length).sum,
        x.stageMap.size
      ))    //end position
      .toBuffer.sortWith( (a, b) => a._1 < b._1)
      .foreach( x => {
        val (jobID, start, end, totalTasks, stages) = x
        out.print(f"[JOBID ${jobID}%7s ")
        out.print(" " * start.toInt)
        out.print("|" * (end.toInt - start.toInt))
        if (80 > end) {
          out.print(" " * (80 - end.toInt))
        }
        out.println("]")
      })
  }
}

object JobOverlapAnalyzer {

  /**
    * Jobs with same sql.execution.id are run in parallel. For completion estimator to work
    * correctly, we need to treat these group of jobs as a single entity.
    * @param ac
    * @return
    */

  def makeJobLists(ac: AppContext): List[List[JobTimeSpan]] = {
    //lets first find the groups by ExecID
    val jobGroupListByExecID = ac.jobSQLExecIdMap.groupBy(x => x._2).map(x => x._2.map(x => ac.jobMap(x._1)).toList).toList

    val allJobIDs = ac.jobMap.map(x => x._1).toSet
    val groupJobIDs = jobGroupListByExecID.flatMap(x => x.map(x => x.jobID)).toSet
    //these jobs are not part of any ExecID
    val leftJobIDs = allJobIDs.diff(groupJobIDs)

    //Merging the two together
    val mergedJobGroupList = new ListBuffer[List[JobTimeSpan]]
    for (elem <- jobGroupListByExecID) {
      mergedJobGroupList.append(elem)
    }

    for (elem <- leftJobIDs) {
      val jobTimeSpan = ac.jobMap(elem)
      val singleItemList = new ListBuffer[JobTimeSpan]
      singleItemList.append(jobTimeSpan)
      mergedJobGroupList.append(singleItemList.toList)
    }
    //we have noticed that jobs withing the same ExecID also have some
    //dependency structure. Not all of the jobs run in parallel. We will
    //try to find isolated/sequential groups here and de-group them.
    val finalJobGroupList = new ListBuffer[List[JobTimeSpan]]
    for (elem <- mergedJobGroupList) {
      if (elem.size == 1) {
        finalJobGroupList.append(elem)
      } else {
        val newLists = splitListIfAppropriate(elem)
        finalJobGroupList.appendAll(newLists)
      }
    }
    finalJobGroupList.toList
  }

  def estimatedTimeSpentInJobs(ac: AppContext): Long = {
    val jobGroupsList = makeJobLists(ac)
    jobGroupsList.map(x => (x.map(x => x.endTime).max - x.map(x => x.startTime).min)).sum
  }

  def criticalPathForAllJobs(ac: AppContext):Long = {
    makeJobLists(ac).map( group => group.map(job => job.computeCriticalTimeForJob()).max).sum
  }

  private def splitListIfAppropriate(list: List[JobTimeSpan]): List[List[JobTimeSpan]] = {
    val newList = new ListBuffer[List[JobTimeSpan]]

    var tempList = new ListBuffer[JobTimeSpan]
    for (job <- list.sortWith((a, b) => a.startTime < b.startTime)) {
      if (tempList.isEmpty) {
        tempList.append(job)
      } else {
        if (tempList.last.endTime < job.startTime) {
          //serial
          newList.append(tempList.toList)
          tempList = new ListBuffer[JobTimeSpan]
          tempList.append(job)
        } else {
          //may be parallel
          tempList.append(job)
        }
      }
    }
    if (!tempList.isEmpty) {
      newList.append(tempList.toList)
    }
    newList.toList
  }
}


