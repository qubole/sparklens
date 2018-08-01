
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
import com.qubole.sparklens.timespan.ExecutorTimeSpan

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/*
 * Created by rohitk on 21/09/17.
 */
class ExecutorTimelineAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()

    out.println("\nPrinting executors timeline....\n")
    out.println(s"Total Executors ${ac.executorMap.size}, " +
      s"and maximum concurrent executors = ${AppContext.getMaxConcurrent(ac.executorMap, ac)}")

    val minuteExecutorMap = new mutable.HashMap[String, (ListBuffer[ExecutorTimeSpan], ListBuffer[ExecutorTimeSpan])]()

    ac.executorMap.values
      .foreach( x => {
        val startMinute = MINUTES_DF.format(x.startTime)
        val minuteLists = minuteExecutorMap.getOrElseUpdate(startMinute, (new mutable.ListBuffer[ExecutorTimeSpan](), new mutable.ListBuffer[ExecutorTimeSpan]()))
        minuteLists._1 += x
        if (x.endTime != 0) {
          val endMinute = MINUTES_DF.format(x.endTime)
          val minuteEndList = minuteExecutorMap.getOrElse(endMinute, (new mutable.ListBuffer[ExecutorTimeSpan](), new mutable.ListBuffer[ExecutorTimeSpan]()))
          minuteEndList._2 += x
        }
      })

    var currentCount = 0
    minuteExecutorMap.keys.toBuffer
      .sortWith( (a, b) => a < b)
      .foreach( x => {
        currentCount = currentCount  + minuteExecutorMap(x)._1.size -  minuteExecutorMap(x)._2.size
        out.println (s"At ${x} executors added ${minuteExecutorMap(x)._1.size} & removed  ${minuteExecutorMap(x)._2.size} currently available ${currentCount}")
      })

    out.println("\nDone printing executors timeline...\n============================\n")
    out.toString()
  }
}
