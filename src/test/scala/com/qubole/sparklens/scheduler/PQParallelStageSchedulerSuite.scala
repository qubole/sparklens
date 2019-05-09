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

import com.qubole.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class PQParallelStageSchedulerSuite extends FunSuite {


    def createStageTimeSpan(stageID: Int, taskCount: Int, minTaskLaunchTime: Long, maxTaskFinishTime: Long, parentStages: Seq[Int]): StageTimeSpan = {
        val stageTimeSpan = new StageTimeSpan(stageID, taskCount)
        stageTimeSpan.minTaskLaunchTime = minTaskLaunchTime
        stageTimeSpan.maxTaskFinishTime = maxTaskFinishTime

        stageTimeSpan.taskExecutionTimes = new Array[Int](taskCount)
        stageTimeSpan.taskPeakMemoryUsage = new Array[Long](taskCount)
        for (i <- 0 to taskCount - 1) {
            stageTimeSpan.taskExecutionTimes(i) = 1
            stageTimeSpan.taskPeakMemoryUsage(i) = 1024 * 1024
        }
        stageTimeSpan.parentStageIDs = parentStages
        stageTimeSpan
    }

    def createJobTimeSpan(id: Int, stages: List[(Int, Int, Long, Long, Seq[Int])]): JobTimeSpan = {
        val jobTimeSpan = new JobTimeSpan(id)
        stages.foreach(x => {
            val stageTimeSpan = createStageTimeSpan(x._1, x._2, x._3, x._4, x._5)
            jobTimeSpan.addStage(stageTimeSpan)
        })
        jobTimeSpan
    }

    test("ParallelTaskSchedulerTest: 1 Job 1 Stage 1 Task 1 Core") {
        val jobTimeSpan = createJobTimeSpan(0, (0, 1, 0L, 1L, Seq.empty[Int]) :: Nil)
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 1)
        info(s"Estimated time $time")
        assert(time === 1, s"time is $time")
    }

    test("ParallelTaskSchedulerTest: 1 Job 1 Stage 1 Task 1 : 1 .. 10 Cores ") {
        val jobTimeSpan = createJobTimeSpan(0, (0, 1, 0L, 1L, Seq.empty[Int]) :: Nil)
        val time1 = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 1)
        for (i <- 2 to 10) {
            val time2 = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, i)
            info(s"Estimated time $time1 $time2")
            assert(time1 === time2, s"Test failed $time1 != $time2")
        }
    }

    test("ParallelTaskSchedulerTest: 2 stages in serial ") {
        val jobTimeSpan = createJobTimeSpan(0, (0, 1, 0L, 1L, Seq.empty[Int]) :: (1, 1, 2L, 3L, 0 :: Nil) :: Nil)
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 1)
        info(s"Estimated time $time")
        assert(time === 2, s"Test failed")
    }

    def makeNStagesInSerialOrder(n: Int): List[(Int, Int, Long, Long, Seq[Int])] = {
        val listBuffer = ListBuffer.empty[(Int, Int, Long, Long, Seq[Int])]
        for (i <- 0 to n) {
            val each = {
                if (i > 0) {
                    (i, 1, i.toLong, (i + 1).toLong, i - 1 :: Nil)
                } else {
                    (i, 1, i.toLong, (i + 1).toLong, Nil)
                }
            }
            listBuffer.append(each)
        }
        listBuffer.toList
    }

    test("ParallelTaskSchedulerTest: 11 stages in serial. 1 Core  ") {
        val jobTimeSpan = createJobTimeSpan(0, makeNStagesInSerialOrder(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 1)
        info(s"Estimated time $time")
        assert(time === 11, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 11 stages in serial. 2 cores  ") {
        //Changing number of cores has no impact on total runtime
        val jobTimeSpan = createJobTimeSpan(0, makeNStagesInSerialOrder(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 2)
        info(s"Estimated time $time")
        assert(time === 11, s"Test failed")
    }

    /**
      *  final stage -- dependent on all the other stages which can run in parallel
      */
    def makeNIndependentStages(n: Int): List[(Int, Int, Long, Long, Seq[Int])] = {
        val listBuffer = ListBuffer.empty[(Int, Int, Long, Long, Seq[Int])]
        val parentList = ListBuffer.empty[Int]
        for (i <- 0 to n-1) {
            val each = (i, 1, i.toLong, (i + 1).toLong, Nil)
            listBuffer.append(each)
            parentList.append(i)
        }
        //make the final stage
        listBuffer.append((n, 1, n.toLong, (n + 1).toLong, parentList))
        listBuffer.toList
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 1 Core") {
        //each stage runs one after the other, just 1 core
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        info (s" $jobTimeSpan")
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 1)
        info(s"Estimated time $time")
        assert(time === 11, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 2 Cores") {
        //2 stages can run in parallel. 10/2+1 = 6
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 2)
        info(s"Estimated time $time")
        assert(time === 6, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 3 Cores") {
        //3 stages can run in parallel. 10/3+1 = 5
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 3)
        info(s"Estimated time $time")
        assert(time === 5, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 4 Cores") {
        //3 stages can run in parallel. 10/4+1 = 4
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 4)
        info(s"Estimated time $time")
        assert(time === 4, s"Test failed")
    }


    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 5 Cores") {
        //3 stages can run in parallel. 10/5+1 = 3
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 5)
        info(s"Estimated time $time")
        assert(time === 3, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 6 Cores") {
        //3 stages can run in parallel. 10/6+1 = 3
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 6)
        info(s"Estimated time $time")
        assert(time === 3, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 7 Cores") {
        //3 stages can run in parallel. 10/7+1 = 3
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 7)
        info(s"Estimated time $time")
        assert(time === 3, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 8 Cores") {
        //3 stages can run in parallel. 10/8+1 = 3
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 8)
        info(s"Estimated time $time")
        assert(time === 3, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 9 Cores") {
        //3 stages can run in parallel. 10/9+1 = 3
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 9)
        info(s"Estimated time $time")
        assert(time === 3, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 10 Cores") {
        //3 stages can run in parallel. 10/10+1 = 2
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 10)
        info(s"Estimated time $time")
        assert(time === 2, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 11 Cores") {
        //3 stages can run in parallel. 10/11+1 = 2
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 11)
        info(s"Estimated time $time")
        assert(time === 2, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 1+10 stages in parallel. 100 Cores") {
        //3 stages can run in parallel. 10/100+1 = 2
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(10))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 100)
        info(s"Estimated time $time")
        assert(time === 2, s"Test failed")
    }


    test("ParallelTaskSchedulerTest: 1+100 stages in parallel. 100 Cores") {
        //3 stages can run in parallel. 100/100+1 = 2
        val jobTimeSpan = createJobTimeSpan(0, makeNIndependentStages(100))
        val time = CompletionEstimator.estimateJobWallClockTime(jobTimeSpan, 1, 100)
        info(s"Estimated time $time")
        assert(time === 2, s"Test failed")
    }

    test("ParallelTaskSchedulerTest: 2 Jobs running in parallel") {
        val jobSQLExecIDMap = new mutable.HashMap[Long, Long]
        val r = scala.util.Random
        val sqlExecutionId = r.nextInt(10000)

        // Let, Job 0, 1 and 2 have same sqlExecutionId
        jobSQLExecIDMap(0) = sqlExecutionId
        jobSQLExecIDMap(1) = sqlExecutionId
        jobSQLExecIDMap(2) = sqlExecutionId
        jobSQLExecIDMap(3) = r.nextInt(10000)

        // Let, Job 1 and 2 are not running in parallel, even though they have same sqlExecutionId
        val jobTimeSpan0 = createJobTimeSpan(0, (0, 1, 0L, 1L, Seq.empty[Int]) :: Nil)
        jobTimeSpan0.setStartTime(0L)
        jobTimeSpan0.setEndTime(1L)
        val jobTimeSpan1 = createJobTimeSpan(1, (0, 1, 0L, 1L, Seq.empty[Int]) :: Nil)
        jobTimeSpan1.setStartTime(0L)
        jobTimeSpan1.setEndTime(1L)
        val jobTimeSpan2 = createJobTimeSpan(2, (0, 1, 2L, 3L, Seq.empty[Int]) :: Nil)
        jobTimeSpan2.setStartTime(2L)
        jobTimeSpan2.setEndTime(3L)
        val jobTimeSpan3 = createJobTimeSpan(3, (0, 1, 4L, 5L, Seq.empty[Int]) :: Nil)
        jobTimeSpan3.setStartTime(4L)
        jobTimeSpan3.setEndTime(5L)

        val jobMap = new mutable.HashMap[Long, JobTimeSpan]
        jobMap(0) = jobTimeSpan0
        jobMap(1) = jobTimeSpan1
        jobMap(2) = jobTimeSpan2
        jobMap(3) = jobTimeSpan3

        val ac = new AppContext(new ApplicationInfo(),
            new AggregateMetrics(),
            mutable.HashMap[String, HostTimeSpan](),
            mutable.HashMap[String, ExecutorTimeSpan](),
            jobMap,
            jobSQLExecIDMap,
            mutable.HashMap[Int, StageTimeSpan](),
            mutable.HashMap[Int, Long]())

        val time = CompletionEstimator.estimateAppWallClockTimeWithJobLists(ac, 1, 1, 3)
        assert(time === 3, s"Test failed")
    }
}
