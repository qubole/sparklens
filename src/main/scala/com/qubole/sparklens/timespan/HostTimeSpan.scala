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

package com.qubole.sparklens.timespan

import com.qubole.sparklens.common.AggregateMetrics
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

import scala.collection.mutable


class HostTimeSpan(val hostID: String) extends TimeSpan {
  var hostMetrics = new AggregateMetrics()

/*
We don't get any event when host is lost.
TODO: may be mark all host end time when execution is stopped
 */
  override def duration():Option[Long] = {
    Some(super.duration().getOrElse(System.currentTimeMillis() - startTime))
  }

  def updateAggregateTaskMetrics (taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
    hostMetrics.update(taskMetrics, taskInfo)
  }
  override def getMap(): Map[String, _ <: Any] = {
    implicit val formats = DefaultFormats
    Map("hostID" -> hostID, "hostMetrics" -> hostMetrics.getMap) ++ super.getStartEndTime()
  }

}

object HostTimeSpan {
  def getTimeSpan(json: Map[String, JValue]): mutable.HashMap[String, HostTimeSpan] = {
    implicit val formats = DefaultFormats
    val map = new mutable.HashMap[String, HostTimeSpan]

    json.keys.map(key => {
      val value = json.get(key).get
      val timeSpan = new HostTimeSpan((value \ "hostID").extract[String])
      timeSpan.hostMetrics = AggregateMetrics.getAggregateMetrics((value \ "hostMetrics")
        .extract[JValue])
      timeSpan.addStartEnd(value)
      map.put(key, timeSpan)
    })

    map
  }
}
