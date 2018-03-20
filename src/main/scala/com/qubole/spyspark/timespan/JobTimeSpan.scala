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

package com.qubole.spyspark.timespan

import com.qubole.spyspark.common.AggregateMetrics
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import scala.collection._


/*
* The timeSpan of a Job can seen with respect to other jobs as well
* as driver timeSpans providing a timeLine. The other way to look at
* Job timeline is to dig deep and check how the individual stages are
* doing
*
* @param jobID
*/

class JobTimeSpan(val jobID: Long) extends TimeSpan {
  val jobMetrics = new AggregateMetrics()
  val stageMap = new mutable.HashMap[Int, StageTimeSpan]()

  def addStage(stage: StageTimeSpan): Unit = {
    stageMap (stage.stageID) = stage
  }
  def updateAggregateTaskMetrics (taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
    jobMetrics.update(taskMetrics, taskInfo)
  }

  /*
  This function computes the minimum time it would take to run this job.
  The computation takes into account the parallel stages.
   */
  def computeCriticalTimeForJob(): Long = {
    val maxStageID = stageMap.map(x => x._1).max
    val data  = stageMap.map(x =>
      ( x._1,
        (
          x._2.parentStageIDs,
          x._2.stageMetrics.map(AggregateMetrics.executorRuntime).max
        )
      )
    )
    criticalTime(maxStageID, data)
  }

  /*
  recursive function to compute critical time starting from the last stage
   */
  private def criticalTime(stageID: Int, data: Map[Int, (Seq[Int], Long)]): Long = {
    //Provide 0 value for
    val stageData = data.getOrElse(stageID, (List.empty[Int], 0L))
    stageData._2 + {
      if (stageData._1.size == 0) {
        0L
      }else {
        stageData._1.map(x => criticalTime(x, data)).max
      }
    }
  }
}
