
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
class StageOverlapAnalyzer extends  AppAnalyzer {

  /*
  TODO: Delete this code. We handle parallel stages now
   */
  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()
    out.println ("\nChecking for stage overlap...\n")
    val conflictingStages = new mutable.ListBuffer[( Long, Int, Int)]
    val jobids = ac.jobMap.keySet.toBuffer.sortWith( _ < _ )
    jobids.foreach( jobID => {
      val jobTimeSpan = ac.jobMap(jobID)
      val stageids = jobTimeSpan.stageMap.keySet.toBuffer.sortWith( _ < _ )

      var lastStageEndTime = 0L
      var lastStageID = 0
      stageids.foreach( stageID => {
        val sts = jobTimeSpan.stageMap(stageID)
        if (sts.endTime > 0 && sts.startTime > 0 ) {
          if (sts.startTime < lastStageEndTime) {
            conflictingStages += ((jobID, stageID, lastStageID))
          }
          lastStageEndTime = sts.endTime
          lastStageID = stageID
        }
      })
    })

    if (conflictingStages.size > 0) {
      out.println(s"Found ${conflictingStages.size} overlapping stages. Some calculations might not be reliable.")
      conflictingStages.foreach( x => {
        out.println(s"In Job ${x._1} stages ${x._2} & ${x._3}")
      })
    }else {
      out.println ("\nNo overlapping stages found. Good\n")
    }
    out.println("\n")
    out.toString()
  }
}
