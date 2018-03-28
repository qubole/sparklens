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
package com.qubole.sparklens.common

import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}

import scala.collection.mutable

case class AppContext(appInfo:        ApplicationInfo,
                      appMetrics:     AggregateMetrics,
                      hostMap:        mutable.HashMap[String, HostTimeSpan],
                      executorMap:    mutable.HashMap[String, ExecutorTimeSpan],
                      jobMap:         mutable.HashMap[Long, JobTimeSpan],
                      stageMap:       mutable.HashMap[Int, StageTimeSpan],
                      stageIDToJobID: mutable.HashMap[Int, Long]) {

  def filterByStartAndEndTime(startTime: Long, endTime: Long): AppContext = {
    new AppContext(appInfo,
      appMetrics,
      hostMap,
      executorMap
        .filter{ case (_, timeSpan) => timeSpan.endTime == 0 ||            //still running
                     timeSpan.endTime >= startTime ||    //ended while app was running
                     timeSpan.startTime <= endTime
        },     //started while app was running
      jobMap
        .filter{ case(_, timeSpan) =>
          timeSpan.startTime >= startTime &&
                  timeSpan.endTime <= endTime
        },
      stageMap
        .filter{ case (_, timeSpan) => timeSpan.startTime >= startTime &&
                    timeSpan.endTime <= endTime
        },
      stageIDToJobID)
  }

}

