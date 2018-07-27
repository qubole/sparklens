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

class ExecutorTimeSpan(val executorID: String,
                       val hostID: String,
                       val cores: Int) extends TimeSpan {
  var executorMetrics = new AggregateMetrics()

  def updateAggregateTaskMetrics (taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
    executorMetrics.update(taskMetrics, taskInfo)
  }

  override def getMap(): Map[String, _ <: Any] = {
    implicit val formats = DefaultFormats

    Map("executorID" -> executorID, "hostID" -> hostID, "cores" -> cores, "executorMetrics" ->
      executorMetrics.getMap()) ++ super.getStartEndTime()
  }
}

object ExecutorTimeSpan {
  def getTimeSpan(json: Map[String, JValue]): mutable.HashMap[String, ExecutorTimeSpan] = {

    implicit val formats = DefaultFormats
    val map = new mutable.HashMap[String, ExecutorTimeSpan]

    json.keys.map(key => {
      val value = json.get(key).get
      val timeSpan = new ExecutorTimeSpan(
        (value \ "executorID").extract[String],
        (value \ "hostID").extract[String],
        (value \ "cores").extract[Int]
      )
      timeSpan.executorMetrics = AggregateMetrics.getAggregateMetrics((value
              \ "executorMetrics").extract[JValue])
      timeSpan.addStartEnd(value)
      map.put(key, timeSpan)
    })
    map
  }
}