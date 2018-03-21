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

package com.qubole.sparklens.scheduler

import scala.collection.mutable

/**
  * TaskScheduler implementation using PriorityQueue (Min)
 */

case class QueuedTask(duration: Int, finishingTime: Long, stageID: Int) extends Ordered[QueuedTask] {
  def compare(that: QueuedTask) = that.finishingTime.compareTo(finishingTime)
}


class PQParallelStageScheduler(totalCores: Int, taskCountMap: mutable.HashMap[Int, Int]) extends TaskScheduler {
  val NO_STAGE_ID = -1
  if (totalCores <= 0) throw new RuntimeException(s"Absurd number of cores ${totalCores}")
  val taskQueue = mutable.PriorityQueue.newBuilder[QueuedTask]
  var wallClock: Long = 0

  /**
    * deques one task from the taskQueue and updates the wallclock time to
    * account for completion of this task.
    *
    * @return if the task is the last task of any stage, it returns the stageID
    *         o.w. returns NO_STAGE_ID (-1)
    */
  private def dequeOne(): Int = {
    var finishedStageID = NO_STAGE_ID
    val finishedTask = taskQueue.dequeue()
    wallClock = finishedTask.finishingTime
    var pendingTasks = taskCountMap(finishedTask.stageID)
    pendingTasks -= 1
    taskCountMap(finishedTask.stageID)= pendingTasks
    //all tasks of checkingStage finished
    if (pendingTasks == 0) {
      onStageFinished(finishedTask.stageID)
      finishedStageID  = finishedTask.stageID
    }
    finishedStageID
  }

  /**
    * Schedules this task for execution on a free core. If no free core is
    * found we advance the wallclock time to make space for the new task.
    *
    * @param taskTime  time that this task should take to complete
    * @param stageID   stage which this task belongs to
    */
  override def schedule(taskTime: Int, stageID: Int): Unit = {
    if (taskQueue.size == totalCores) {
      dequeOne()
    }
    taskQueue.enqueue(new QueuedTask(taskTime, wallClock+taskTime, stageID))
  }


  /**
    * If we run out of tasks, and new tasks will only get submitted if new
    * stages are submitted. We want to finish tasks till the point some
    * stage finishes. This method pushes the simulation to the next interesting
    * point, which is completion of some stage.
   */
  override def runTillStageCompletion():Int = {
    var finishedStageID = NO_STAGE_ID
    while (taskQueue.size > 0 && finishedStageID == NO_STAGE_ID) {
      finishedStageID = dequeOne()
    }
    if (finishedStageID == NO_STAGE_ID) {
      throw new RuntimeException("Unable to finish any stage after handling all scheduled tasks")
    }
    return finishedStageID
  }

  override def isStageComplete(stageID: Int): Boolean = {
    val tasksPending = taskCountMap.getOrElse(stageID, 0)
    tasksPending == 0
  }


  override def wallClockTime(): Long = {
    if (taskQueue.nonEmpty) {
      taskQueue.map(x => x.finishingTime).max
    } else {
      wallClock
    }
  }
}