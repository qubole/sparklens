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

import com.qubole.sparklens.common.AppContext
import com.qubole.sparklens.timespan.JobTimeSpan

import scala.collection.mutable

/*
* The responsibility of computing time needed to process a given application '
* with different number of executors is divided among two classes.
* CompletionEstimator and PQParallelStageScheduler.
*
* CompletionEstimator is responsible for scheduling stages based on DAG
* PQParallelStageScheduler is the task scheduler, which ensures that
* we are not running more tasks than the total nunber of cores in the
* system.
*/

case class EstimatorState(val stagesData:Map[Int,(Array[Int], Seq[Int])]) {
  val waitingStages = new mutable.TreeSet[Int]()
  val runnableStages = new mutable.TreeSet[Int]()
  val runningStages = new mutable.TreeSet[Int]()
}


object CompletionEstimator {
  private def scheduleStage(stageID: Int, estate :EstimatorState, scheduler: PQParallelStageScheduler): Unit = {
    val stageData = estate.stagesData.getOrElse(stageID, (Array.emptyIntArray, List.empty[Int]))
    if (stageData._1.length > 0) {
      if (stageData._2.isEmpty) {
        //no parents
        estate.runnableStages += stageID
      }else {
        // some parents
        val nonSkippedParents = stageData._2.filter(parentStage => estate.stagesData.get(parentStage).isDefined).size
        if (nonSkippedParents > 0) {
          estate.waitingStages += stageID
          stageData._2.foreach( parentStage => {
            scheduleStage(parentStage, estate, scheduler)
          })
        }else {
          estate.runnableStages += stageID
        }
      }
    }else {
      println(s"Skipped stage ${stageID} with 0 tasks")
    }
  }

  private def processStages(maxStageID: Int, estate: EstimatorState,  scheduler: PQParallelStageScheduler): Long = {
    //In the worst case we need to push all stages to completion
    val MAX_COMPLETION_TRIES = estate.stagesData.size+1
    var completionRetries = 0
    while (!scheduler.isStageComplete(maxStageID) && (completionRetries < MAX_COMPLETION_TRIES)) {
      if (estate.runnableStages.nonEmpty) {
        val copyOfRunningStages = estate.runnableStages.clone()
        val eligibleStages = copyOfRunningStages.filterNot(stageID => estate.runningStages.contains(stageID))
        if (eligibleStages.size > 0) {
          val currentStageID = eligibleStages.toList(0)
          estate.runningStages += currentStageID
          estate.runnableStages -= currentStageID
          estate.stagesData.getOrElse(currentStageID, (Array.emptyIntArray, List.empty[Int]))
            ._1.foreach(taskTime => {
            if (taskTime <= 0) {
              //force each task to be at least 1 milli second
              //scheduler doesn't work with 0 or negative millis second tasks
              scheduler.schedule(1, currentStageID)
            }else {
              scheduler.schedule(taskTime, currentStageID)
            }
          })
        }
      }else {
        //we have dependency to finish first
        scheduler.runTillStageCompletion()
        completionRetries += 1
      }
    }
    if (completionRetries >= MAX_COMPLETION_TRIES) {
      println(s"ERROR: Estimation of job completion time aborted $completionRetries")
      println(s"runnableStages ${estate.runnableStages}")
      println(s"runningStages ${estate.runningStages}")
      println(s"waitingStages ${estate.waitingStages}")
      println(s"maxStageID ${maxStageID}")
      0L
    }else {
      scheduler.wallClockTime()
    }
  }

  def estimateJobWallClockTime(jobTimeSpan: JobTimeSpan, executorCount: Int, perExecutorCores:Int): Long = {
    if (jobTimeSpan.stageMap.isEmpty) {
      //Job has no stages and no tasks?
      //we assume that such a job will run in same time irrespective of number of cores
      return jobTimeSpan.duration().getOrElse(0)
    }
    val maxStageID = jobTimeSpan.stageMap.map(x => x._1).max
    val data = jobTimeSpan.stageMap.map(x => (x._1, ( x._2.taskExecutionTimes, x._2.parentStageIDs))).toMap
    val taskCountMap = new mutable.HashMap[Int, Int]()
    data.foreach( x => {
      taskCountMap(x._1) = x._2._1.length
    })
    val estate = new EstimatorState(data)
    val scheduler = new PQParallelStageScheduler(executorCount * perExecutorCores, taskCountMap) {
      override def onStageFinished(stageID: Int ) = {
        estate.runningStages -= stageID
        var nowRunnableStages = estate.waitingStages.filter(eachStage => {
          estate.stagesData(eachStage)._2.forall( parentStage => isStageComplete(parentStage))
        })
        estate.waitingStages --= nowRunnableStages
        estate.runnableStages ++= nowRunnableStages
      }
    }
    scheduleStage(maxStageID, estate, scheduler)
    processStages(maxStageID, estate, scheduler)
  }

  def estimateAppWallClockTime(ac: AppContext, executorCount: Int, perExecutorCores:Int, appRealDuration: Long): Long = {
    val appTotalTime = appRealDuration
    val jobTime  = ac.jobMap.values.filter( x => x.endTime > 0)
      .map(x => (x.endTime - x.startTime))
      .sum
    val driverTimeJobBased = appTotalTime - jobTime

    ac.jobMap.values.map(x =>
      estimateJobWallClockTime(x,
        executorCount,
        perExecutorCores)
    ).sum + driverTimeJobBased
  }
}


