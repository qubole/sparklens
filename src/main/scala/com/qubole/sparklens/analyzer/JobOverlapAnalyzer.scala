
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

import scala.collection.mutable

/*
 * Created by rohitk on 21/09/17.
 */
class JobOverlapAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()
    out.println ("\nChecking for job overlap...\n")
    val conflictingJobs = new mutable.ListBuffer[( Long, Long)]
    val jobids = ac.jobMap.keySet.toBuffer.sortWith( _ < _ )

    var lastJobEndTime = 0L
    var lastJobID = 0L

    jobids.foreach( jobID => {
      val jobTimeSpan = ac.jobMap(jobID)

      if (jobTimeSpan.endTime > 0 && jobTimeSpan.startTime > 0 ) {
        if (jobTimeSpan.startTime < lastJobEndTime) {
          conflictingJobs += ((jobID, lastJobID))
        }
        lastJobEndTime = jobTimeSpan.endTime
        lastJobID = jobID
      }
    })

    if (conflictingJobs.size > 0) {
      out.println(s"Found ${conflictingJobs.size} overlapping Jobs. Using threadpool for submitting parallel jobs? " +
        s"Some calculations might not be reliable.")
      conflictingJobs.foreach( x => {
        out.println(s"Running in parallel:  JobID ${x._1} && JobID ${x._2} ")
      })
    }else {
      out.println ("\nNo overlapping jobs found. Good\n")
    }
    out.println("\n")
    out.toString()
  }
}
