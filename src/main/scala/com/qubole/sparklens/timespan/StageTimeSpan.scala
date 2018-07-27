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
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable

/*
This keeps track of data per stage
*/

class StageTimeSpan(val stageID: Int, numberOfTasks: Long) extends TimeSpan {
  var stageMetrics  = new AggregateMetrics()
  var tempTaskTimes = new mutable.ListBuffer[( Long, Long, Long)]
  var minTaskLaunchTime = Long.MaxValue
  var maxTaskFinishTime = 0L
  var parentStageIDs:Seq[Int] = null

  // we keep execution time of each task
  var taskExecutionTimes  = Array.emptyIntArray
  var taskPeakMemoryUsage = Array.emptyLongArray

  def updateAggregateTaskMetrics (taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
    stageMetrics.update(taskMetrics, taskInfo)
  }

  def setParentStageIDs(parentIDs: Seq[Int]): Unit = {
    parentStageIDs = parentIDs
  }

  def updateTasks(taskInfo: TaskInfo, taskMetrics: TaskMetrics): Unit = {
    if (taskInfo != null && taskMetrics != null) {
      tempTaskTimes += ((taskInfo.taskId, taskMetrics.executorRunTime, taskMetrics.peakExecutionMemory))
      if (taskInfo.launchTime < minTaskLaunchTime) {
        minTaskLaunchTime = taskInfo.launchTime
      }
      if (taskInfo.finishTime > maxTaskFinishTime) {
        maxTaskFinishTime = taskInfo.finishTime
      }
    }
  }

  def finalUpdate(): Unit = {
    //min time for stage is when its tasks started not when it is submitted
    setStartTime(minTaskLaunchTime)
    setEndTime(maxTaskFinishTime)

    taskExecutionTimes = new Array[Int](tempTaskTimes.size)

    var currentIndex = 0
    tempTaskTimes.sortWith(( left, right)  => left._1 < right._1)
      .foreach( x => {
        taskExecutionTimes( currentIndex) = x._2.toInt
        currentIndex += 1
      })

    val countPeakMemoryUsage = {
      if (tempTaskTimes.size > 64) {
         64
      }else {
        tempTaskTimes.size
      }
    }

    taskPeakMemoryUsage = tempTaskTimes
      .map( x => x._3)
      .sortWith( (a, b) => a > b)
      .take(countPeakMemoryUsage).toArray

    /*
    Clean the tempTaskTimes. We don't want to keep all this objects hanging around for
    long time
     */
    tempTaskTimes.clear()
  }

  override def getMap(): Map[String, _ <: Any] = {
    implicit val formats = DefaultFormats

    Map(
      "stageID" -> stageID,
      "numberOfTasks" -> numberOfTasks,
      "stageMetrics" -> stageMetrics.getMap(),
      "minTaskLaunchTime" -> minTaskLaunchTime,
      "maxTaskFinishTime" -> maxTaskFinishTime,
      "parentStageIDs" -> parentStageIDs.mkString("[", ",", "]"),
      "taskExecutionTimes" -> taskExecutionTimes.mkString("[", ",", "]"),
      "taskPeakMemoryUsage" -> taskPeakMemoryUsage.mkString("[", ",", "]")
    ) ++ super.getStartEndTime()
  }
}

object StageTimeSpan {

  def getTimeSpan(json: Map[String, JValue]): mutable.HashMap[Int, StageTimeSpan] = {
    implicit val formats = DefaultFormats

    val map = new mutable.HashMap[Int, StageTimeSpan]

    json.keys.map(key => {
      val value = json.get(key).get
      val timeSpan = new StageTimeSpan(
        (value \ "stageID").extract[Int],
        (value  \ "numberOfTasks").extract[Long]
      )
      timeSpan.stageMetrics = AggregateMetrics.getAggregateMetrics((value \ "stageMetrics")
              .extract[JValue])
      timeSpan.minTaskLaunchTime = (value \ "minTaskLaunchTime").extract[Long]
      timeSpan.maxTaskFinishTime = (value \ "maxTaskFinishTime").extract[Long]


      timeSpan.parentStageIDs = parse((value \ "parentStageIDs").extract[String]).extract[List[Int]]
      timeSpan.taskExecutionTimes = parse((value \ "taskExecutionTimes").extract[String])
        .extract[List[Int]].toArray

      timeSpan.taskPeakMemoryUsage = parse((value \ "taskPeakMemoryUsage").extract[String])
        .extract[List[Long]].toArray

      timeSpan.addStartEnd(value)

      map.put(key.toInt, timeSpan)
    })
    map
  }

}
