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

import java.util.Date
import java.util.concurrent.TimeUnit

import com.qubole.sparklens.analyzer.JobOverlapAnalyzer
import com.qubole.sparklens.common.{AggregateMetrics, AppContext}
import com.qubole.sparklens.timespan.JobTimeSpan
import com.qubole.sparklens.helper.JobOverlapHelper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

  private def processStages(maxStageIDs: List[Int], estate: EstimatorState,  scheduler: PQParallelStageScheduler): Long = {
    //In the worst case we need to push all stages to completion
    val MAX_COMPLETION_TRIES = estate.stagesData.size+1
    var completionRetries = 0
    while (!maxStageIDs
        .map(scheduler.isStageComplete(_))
        .forall(_ == true)
            && (completionRetries < MAX_COMPLETION_TRIES)) {
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
      println(s"maxStageIDs ${maxStageIDs}")
      0L
    }else {
      scheduler.wallClockTime()
    }
  }

  @deprecated
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
    processStages(List(maxStageID), estate, scheduler)
  }


  /*
  Many times multiple jobs are scheduled at the same time. We can identify them from the same sql execution id. This
  method simulates scheduling of these jobs which can run concurrently.
   */
  def estimateJobListWallClockTime(jobTimeSpans: List[JobTimeSpan], executorCount: Int, perExecutorCores:Int): Long = {
    val realJobTimeSpans = jobTimeSpans.filter(x => !x.stageMap.isEmpty)
    //Job has no stages and no tasks?
    //we assume that such a job will run in same time irrespective of number of cores
    var  timeForEmptyJobs:Long = 0
      jobTimeSpans.filter(x => x.stageMap.isEmpty)
      .foreach(x => {
        if (x.duration().isDefined) {
          timeForEmptyJobs += x.duration().get
        }
      })

    if (realJobTimeSpans.isEmpty) {
      return timeForEmptyJobs;
    }

    //Here we combine the data from all the parallel jobs. Scheduler takes the information about stages, time spent in
    //each task and the depedency between stages. This information is added to one data structure, but instead of
    //coming from just one job, we give data from all the parallel jobs
    val data = realJobTimeSpans.flatMap(x => x.stageMap.map(x => (x._1, ( x._2.taskExecutionTimes, x._2.parentStageIDs)))).toMap
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
    //Here we find out the runnable stages to seed the simulation. scheduleStage takes the maxStageID of each job and
    //finds the list of runnable stages by traversing the dependency graph. We need to do this for all the jobs, since
    //multiple stages from different jobs can be runnable, given the parallel nature of jobs
    realJobTimeSpans.map (x => x.stageMap.map(x => x._1).max).foreach( maxStageID => {
      scheduleStage(maxStageID, estate, scheduler)
    })

    //TODO: we might need to revisit this. Not sure if we should work with a single maxStageID now or switch to
    //list of maxStageIDs, one each for each parallel job.
    val maxStageIDs = realJobTimeSpans.map (x => x.stageMap.map(x => x._1).max)
    processStages(maxStageIDs, estate, scheduler)
  }

  @deprecated
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


  /**
    * New simulation method. The way we compute time spent in driver is now a bit different. Instead of just
    * summing up time spent in jobs and substracting it from total application time, we now take into
    * account job level parallelism as defined by sql.execution.id. The earlier method would sometime
    * compute driver time as negative when sum of job times exceeded total application time.
    *
    * Well the logic is a bit more complicated than just using the sql.execution.id. We see jobs with the
    * same sql.execution.id also having some sort of dependencies. These are not captured anywhere in the
    * event log data (or may be I am not aware of it). Nevertheless, what we really do to make the jobGroups
    * is to first group them by sql.execution.id and then split these groups based on actual observed
    * parallelism. We look at jobs within a group sorted by start time, looking at pair at a time. If the
    * pair has some overlap in time, we assume they run in parallel. If we see a clean split, we split the
    * group.  The code is here: JobOverlapHelper.makeJobLists
    *
    * @param ac
    * @param executorCount
    * @param perExecutorCores
    * @param appRealDuration
    * @return
    */
  def estimateAppWallClockTimeWithJobLists(ac: AppContext, executorCount: Int, perExecutorCores:Int, appRealDuration: Long): Long = {
    val appTotalTime = appRealDuration
    val jobGroupsList = JobOverlapHelper.makeJobLists(ac)
    // for each group we take the processing time as min of start time and max of end time from all the
    // jobs that are part of the group
    val jobTime  = JobOverlapHelper.estimatedTimeSpentInJobs(ac)
    val driverTimeJobBased = appTotalTime - jobTime
    jobGroupsList.map(x => {
        estimateJobListWallClockTime(x,
          executorCount,
          perExecutorCores)
    }).sum + driverTimeJobBased
  }
}


