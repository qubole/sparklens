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

import com.google.gson.{Gson, GsonBuilder}
import com.qubole.sparklens.timespan._
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

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
        .filter(x => x._2.endTime == 0 ||            //still running
                     x._2.endTime >= startTime ||    //ended while app was running
                     x._2.startTime <= endTime),     //started while app was running
      jobMap
        .filter(x => x._2.startTime >= startTime &&
                     x._2.endTime <= endTime),
      stageMap
        .filter(x => x._2.startTime >= startTime &&
                     x._2.endTime <= endTime),
      stageIDToJobID)
  }

  override def toString(): String = {
    import scala.collection.JavaConverters._

    val map = Map(
      "appInfo" -> appInfo,
      "appMetrics" -> appMetrics.getJavaMap(),
      "hostMap" -> AppContext.convertMapToJavaMap(hostMap),
      "executorMap" -> AppContext.convertMapToJavaMap(executorMap),
      "jobMap" -> AppContext.convertMapToJavaMap(jobMap),
      "stageMap" -> AppContext.convertMapToJavaMap(stageMap),
      "stageIDToJobID" -> stageIDToJobID.asJava
    ).asJava
    new GsonBuilder().setPrettyPrinting().create().toJson(map)
  }


}

object AppContext {

  def convertMapToJavaMap(map: mutable.HashMap[_ <: Any, _ <: TimeSpan]):
    java.util.Map[String, Any] = {

    import scala.collection.JavaConverters._
    val newMap = new mutable.HashMap[String, Any]
    for ((k, v) <- map) newMap.put(k.toString, v.getJavaMap())
    newMap.asJava
  }

  def getContext(json: JValue): AppContext = {

    implicit val formats = DefaultFormats

    new AppContext(
      ApplicationInfo.getObject((json \ "appInfo").extract[JValue]),
      AggregateMetrics.getAggregateMetrics((json \ "appMetrics").extract[JValue]),
      HostTimeSpan.getTimeSpan((json \ "hostMap").extract[Map[String, JValue]]),
      ExecutorTimeSpan.getTimeSpan((json \ "executorMap").extract[Map[String, JValue]]),
      JobTimeSpan.getTimeSpan((json \ "jobMap").extract[Map[String, JValue]]),
      StageTimeSpan.getTimeSpan((json \ "stageMap").extract[Map[String, JValue]]),
      getJobToStageMap((json \ "stageIDToJobID").extract[Map[Int, JValue]])
    )
}

  private def getJobToStageMap(json: Map[Int, JValue]): mutable.HashMap[Int, Long] = {
    implicit val formats = DefaultFormats
    val map = new mutable.HashMap[Int, Long]()

    json.keys.map(key => {
      map.put(key, json.get(key).get.extract[Long])
    })
    map
  }
}

