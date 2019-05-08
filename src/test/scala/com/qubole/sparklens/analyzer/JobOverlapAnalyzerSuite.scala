
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

package com.qubole.sparklens.analyzer

import com.qubole.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import com.qubole.sparklens.helper.JobOverlapHelper

import org.scalatest.FunSuite

import scala.collection.mutable

class JobOverlapAnalyzerSuite extends FunSuite {

  def createDummyAppContext(): AppContext = {

    val jobMap = new mutable.HashMap[Long, JobTimeSpan]
    for (i <- 1 to 4) {
      jobMap(i) = new JobTimeSpan(i)
    }

    val jobSQLExecIDMap = new mutable.HashMap[Long, Long]
    val r = scala.util.Random
    val sqlExecutionId = r.nextInt(10000)

    // Let, Job 1, 2 and 3 have same sqlExecutionId
    jobSQLExecIDMap(1) = sqlExecutionId
    jobSQLExecIDMap(2) = sqlExecutionId
    jobSQLExecIDMap(3) = sqlExecutionId
    jobSQLExecIDMap(4) = r.nextInt(10000)

    // Let, Job 2 and 3 are not running in parallel, even though they have same sqlExecutionId
    val baseTime = 1L
    jobMap(1).setStartTime(baseTime)
    jobMap(1).setEndTime(baseTime + 5L)

    jobMap(2).setStartTime(baseTime + 3L)
    jobMap(2).setEndTime(baseTime + 6L)

    jobMap(3).setStartTime(baseTime + 7L)
    jobMap(3).setEndTime(baseTime + 9L)

    jobMap(4).setStartTime(baseTime + 10L)
    jobMap(4).setEndTime(baseTime + 12L)

    new AppContext(new ApplicationInfo(),
      new AggregateMetrics(),
      mutable.HashMap[String, HostTimeSpan](),
      mutable.HashMap[String, ExecutorTimeSpan](),
      jobMap,
      jobSQLExecIDMap,
      mutable.HashMap[Int, StageTimeSpan](),
      mutable.HashMap[Int, Long]())
  }

  test("JobOverlapAnalyzerTest: Jobs running in parallel should be considered while computing " +
    "estimated time spent in Jobs") {
    val ac = createDummyAppContext()
    val jobTime = JobOverlapHelper.estimatedTimeSpentInJobs(ac)
    assert(jobTime == 10, "Parallel Jobs are not being considered while computing the time spent in jobs")
  }
}
