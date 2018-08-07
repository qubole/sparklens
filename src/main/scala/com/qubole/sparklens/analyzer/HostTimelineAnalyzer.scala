
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
import com.qubole.sparklens.timespan.HostTimeSpan

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/*
 * Created by rohitk on 21/09/17.
 */
class HostTimelineAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()
    out.println(s"\nTotal Hosts ${ac.hostMap.size}, " +
      s"and the maximum concurrent hosts = ${AppContext.getMaxConcurrent(ac.hostMap, ac)}")
    val minuteHostMap = new mutable.HashMap[Long, ListBuffer[HostTimeSpan]]()
    ac.hostMap.values
      .foreach( x => {
        val startMinute = x.startTime / 60*1000
        val minuteList = minuteHostMap.getOrElse(startMinute, new mutable.ListBuffer[HostTimeSpan]())
        minuteList += x
      })
    minuteHostMap.keys.toBuffer
      .sortWith( (a, b) => a < b)
      .foreach( x => {
        out.println (s"At ${pt(x*60*1000)} added ${minuteHostMap(x).size} hosts ")
      })
    out.println("\n")
    ac.hostMap.values.foreach(x => {
      val executorsOnHost = ac.executorMap.values.filter( _.hostID.equals(x.hostID))
      out.println(s"Host ${x.hostID} startTime ${pt(x.startTime)} executors count ${executorsOnHost.size}")
    })
    out.println("Done printing host timeline\n======================\n")
    out.toString()
  }
}
