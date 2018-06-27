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

import java.util

import com.google.gson.JsonObject
import com.qubole.sparklens.common.AggregateMetrics
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import scala.collection.mutable



class ExecutorTimeSpan(val executorID: String,
                       val hostID: String,
                       val cores: Int) extends TimeSpan {
  var executorMetrics = new AggregateMetrics()

  def updateAggregateTaskMetrics (taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
    executorMetrics.update(taskMetrics, taskInfo)
  }

  override def getJavaMap(): util.Map[String, _ <: Any] = {
    import scala.collection.JavaConverters._
    (Map("executorID" -> executorID, "hostID" -> hostID, "cores" -> cores, "executorMetrics" ->
      executorMetrics.getJavaMap()) ++ super.getStartEndTime()).asJava
  }
}

object ExecutorTimeSpan {
  def getTimeSpan(json: JsonObject): mutable.HashMap[String, ExecutorTimeSpan] = {
    val map = new mutable.HashMap[String, ExecutorTimeSpan]
    import scala.collection.JavaConverters._
    for (elem <- json.entrySet().asScala) {
      val value = elem.getValue.getAsJsonObject
      val timeSpan = new ExecutorTimeSpan(
        value.get("executorID").getAsString,
        value.get("hostID").getAsString,
        value.get("cores").getAsInt)
      timeSpan.executorMetrics = AggregateMetrics.getAggregateMetrics(value.get("executorMetrics")
        .getAsJsonObject)
      timeSpan.addStartEnd(value)
      map.put(elem.getKey, timeSpan)
    }
    map
  }
}