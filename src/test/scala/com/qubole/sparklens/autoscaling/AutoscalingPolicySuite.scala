package com.qubole.sparklens.autoscaling

import com.qubole.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import org.apache.spark.{DummyExecutorAllocator, SparkConf}
import org.apache.spark.autoscaling.DummyAutoscalingSparklensClient
import org.apache.spark.scheduler.{SparkListenerExecutorAdded, SparkListenerExecutorRemoved, SparkListenerJobEnd, SparkListenerJobStart}
import org.scalatest.FunSuite

import scala.collection.mutable

class AutoscalingPolicySuite extends FunSuite {

  /**
    * These tests donot have the time at which job starts and ends automated. Automating this
    * would make these tests better.
    */
  test("basic test") {
    /* List with `(jobId, startTime, endTime, optimumExecutors)` */
    val proxyJobList = List(List(1L, 1, 10, 2), List(2L, 11, 20, 4), List(3L, 21, 30, 1))

    val (policy, client) = getPolicy(proxyJobList)

    // check that two executors have been added when job 1 starts
    policy.scale(new SparkListenerJobStart(1, 0, Seq.empty))
    assert (client.updateRequests.size == 1)
    assert (client.updateRequests.last == 2)

    // assert nothing changes when job 1 ends.
    policy.scale(new SparkListenerJobEnd(1, 0, null))
    assert (client.updateRequests.size == 1)
    assert (client.killRequests.size == 0)

    // assert total of 4 executors are requested when 2nd job starts
    policy.scale(new SparkListenerJobStart(2, 0, Seq.empty))
    assert (client.updateRequests.size == 2)
    assert (client.updateRequests.last == 4)

    // assert nothing changes when job 2 ends
    policy.scale(new SparkListenerJobEnd(2, 0, null))
    assert (client.updateRequests.size == 2)
    assert (client.killRequests.size == 0)

    // add all the requested executors. Should be done before removal of executors is tried.
    policy.onExecutorAdded(new SparkListenerExecutorAdded(0, "1", null))
    policy.onExecutorAdded(new SparkListenerExecutorAdded(0, "2", null))
    policy.onExecutorAdded(new SparkListenerExecutorAdded(0, "3", null))
    policy.onExecutorAdded(new SparkListenerExecutorAdded(0, "4", null))


    // assert 2 executors have been asked to released when job 3 starts
    policy.scale(new SparkListenerJobStart(3, 0, Seq.empty))
    assert (client.killRequests.size == 1)
    assert (client.killRequests.last.size == 3)
    assert (client.updateRequests.size == 3)
    assert (client.updateRequests.last == 1)

    // assert that remaining 1 executor is also removed after job 3 (last job) is complete.
    policy.scale(new SparkListenerJobEnd(3, 0, null))
    Thread.sleep(AutoscalingPolicy.releaseTimeout * 4)
    assert (client.killRequests.size == 2)
    assert (client.killRequests.last.size == 1)

  }

  test ("executors get removed if enough gap between jobs") {
    /* List with `(jobId, startTime, endTime, optimumExecutors)` */
    val proxyJobList = List(List(1L, 1, 10, 10),
      List(2L, AutoscalingPolicy.releaseTimeout + 50, AutoscalingPolicy.releaseTimeout + 60, 4))
    val (policy, client) = getPolicy(proxyJobList)

    // Start and end the first job
    policy.scale(new SparkListenerJobStart(1, 0, Seq.empty))
    (1 to 10).foreach(x => policy.onExecutorAdded(new SparkListenerExecutorAdded(0, x.toString, null)))
    policy.scale(new SparkListenerJobEnd(1, 0, null))

    // check all executors have been asked to release
    Thread.sleep(AutoscalingPolicy.releaseTimeout * 4)
    assert (client.killRequests.size == 1)
    assert (client.killRequests.last.size == 10)

    // when new job starts, it should ask for 4 more executors
    policy.scale(new SparkListenerJobStart(2, 0, Seq.empty))
    assert (client.updateRequests.last == 4)
    assert (client.killRequests.size == 1)
  }

  test ("No more requests to allocator client if previous requests have already been made") {
    /* List with `(jobId, startTime, endTime, optimumExecutors)` */
    val proxyJobList = List(List(1L, 1, 10, 4), List(2L, 11, 21, 4))
    val (policy, client) = getPolicy(proxyJobList)

    // Start and end job 1 , but no executors have yet been provided.
    policy.scale(new SparkListenerJobStart(1, 0, Seq.empty))
    policy.scale(new SparkListenerJobEnd(1, 0, null))
    assert (client.updateRequests.size == 1)
    assert (client.updateRequests.last == 4)

    // start job 2, but no new request to client since 4 executors have already been asked
    policy.scale(new SparkListenerJobStart(2, 0 , Seq.empty))
    assert (client.updateRequests.size == 1)
  }


  test ("Asking for killing of executors should be irrespective of how many executors have " +
    "actually been killed by the client") {
    /* List with `(jobId, startTime, endTime, optimumExecutors)` */
    val proxyJobList = List(List(1L, 1, 10, 20), List(2L, 11, 20, 10), List(3L, 21, 30, 5), List
    (4L, 31, 40, 2))
    val (policy, client) = getPolicy(proxyJobList)

    // start and end job 1
    policy.scale(new SparkListenerJobStart(1, 0, Seq.empty))
    policy.scale(new SparkListenerJobEnd(1, 0, null))
    (1 to 20).foreach(x => policy.onExecutorAdded(new SparkListenerExecutorAdded(0, x.toString, null)))

    // start job 2, it will ask to kill 10 executors, but donot kill them yet.
    policy.scale(new SparkListenerJobStart(2, 0, Seq.empty))
    assert (client.killRequests.size == 1)
    assert (client.killRequests.last.size == 10)

    // finish job 2. start job 3. More kill requests come, but even earlier executors have not
    // been killed
    policy.scale(new SparkListenerJobEnd(2, 0, null))
    policy.scale(new SparkListenerJobStart(3, 0, Seq.empty))
    assert (client.killRequests.size == 2)
    assert (client.killRequests.last.size == 5)
    assert (client.killRequests.head.toSet.intersect(client.killRequests.last.toSet).size == 0)

    // finish job 3. Some executors from both previous remove request are completed, but not all.
    policy.scale(new SparkListenerJobEnd(3, 0, null))
    List(client.killRequests.head, client.killRequests.last).foreach(_.take(2).foreach(x => {
      policy.onExecutorRemoved(new SparkListenerExecutorRemoved(0, x.toString, null))
    }))

    // despite these random executor removals, asking client for executor removal should be
    // consistent
    policy.scale(new SparkListenerJobStart(4, 0, Seq.empty))
    assert (client.killRequests.size == 3)
    assert (client.killRequests.last.size == 3) // executors decreased from 5 to 2.
    assert (client.killRequests.last.toSet.intersect(
      client.killRequests.slice(0,1).flatMap(x => x).toSet).size == 0)


    // remove remaining 2 executors after complete of 4th job
    policy.scale(new SparkListenerJobEnd(4, 0, null))
    Thread.sleep(AutoscalingPolicy.releaseTimeout * 4)
    assert (client.killRequests.size == 4)
    assert (client.killRequests.last.size == 2)

    assert (client.killRequests.last.toSet.intersect(
      client.killRequests.slice(0,2).flatMap(x => x).toSet).size == 0)
  }

  test ("donot upscale beyond max") {
    /* List with `(jobId, startTime, endTime, optimumExecutors)` */
    val proxyJobList = List(List(1L, 1, 10, 2000))
    val (policy, client) = getPolicy(proxyJobList)

    policy.scale(new SparkListenerJobStart(1, 0, Seq.empty))

    assert (client.updateRequests.size == 1)
    assert (client.updateRequests.last == 1000)
  }

  test ("parallel jobs") {
    val proxyJobList = List(List(1L, 1, 20, 20), List(2L, 11, 30, 10), List(3L, 15, 30, 5), List
    (4L, 31 + AutoscalingPolicy.releaseTimeout, 40, 2))
    val (policy, client) = getPolicy(proxyJobList)

    // start job 1, check 20 executors
    policy.scale(new SparkListenerJobStart(1, 0, Seq.empty))
    assert (client.updateRequests.size == 1)
    assert (client.updateRequests.last == 20)

    // start job 2, check for 30 executors total now.
    policy.scale(new SparkListenerJobStart(2, 0, Seq.empty))
    assert (client.updateRequests.size == 2)
    assert (client.updateRequests.last == 30)

    // start job 3, check for 35 executors total
    policy.scale(new SparkListenerJobStart(3, 0, Seq.empty))
    assert (client.updateRequests.size == 3)
    assert (client.updateRequests.last == 35)

    // Add all the executors
    (1 to 35).foreach(x => policy.onExecutorAdded(new SparkListenerExecutorAdded(0, x.toString, null)))

    // complete job 1, immediately remove corresponding executors
    policy.scale(new SparkListenerJobEnd(1, 0, null))
    assert (client.killRequests.size == 1)
    assert (client.killRequests.last.size == 20)

    // complete job 2, immediately remove corresponding executors
    policy.scale(new SparkListenerJobEnd(2, 0, null))
    assert (client.killRequests.size == 2)
    assert (client.killRequests.last.size == 10)

    // complete job 3, donot see that kill request has come for this
    policy.scale(new SparkListenerJobEnd(3, 0, null))
    assert (client.killRequests.size == 2)

    // after waiting for release timeout, this should be released
    Thread.sleep(AutoscalingPolicy.releaseTimeout * 4)
    assert (client.killRequests.size == 3)
    assert (client.killRequests.last.size == 5)

  }

  private def getPolicy(proxyJobList: List[List[Long]]):
  (AutoscalingPolicy, DummyAutoscalingSparklensClient) = {

    AutoscalingPolicy.setTimeoutForUnitTest(100)
    val appInfo = new ApplicationInfo()
    appInfo.startTime = 0

    val client = new DummyAutoscalingSparklensClient(new DummyExecutorAllocator())
    val executorId = "dummyExecutorId"
    val execTimeSpan = new ExecutorTimeSpan(executorId, "dummyHostId", 4)

    val jobList = proxyJobList.sortWith(_.head < _.head).map(job => {
      val jobTimeSpan = new DummyJobTimeSpan(job.head, job.last.asInstanceOf[Int])
      jobTimeSpan.startTime = job(1)
      jobTimeSpan.endTime = job(2)
      (job.head -> jobTimeSpan)
    })
    val jobMap = collection.mutable.HashMap(jobList: _*).asInstanceOf[mutable.HashMap[Long, JobTimeSpan]]

    val executorMap = collection.mutable.HashMap(Seq(executorId -> execTimeSpan): _*)
      .asInstanceOf[mutable.HashMap[String, ExecutorTimeSpan]]
    val ac = new AppContext(appInfo,
      new AggregateMetrics,
      mutable.HashMap.empty[String, HostTimeSpan],
      executorMap,
      jobMap,
      mutable.HashMap.empty[Int, StageTimeSpan],
      mutable.HashMap.empty[Int, Long]
    )
    (new AutoscalingPolicy(client, new SparkConf().set("spark.dynamicAllocation.maxExecutors",
      "1000"),
      Some(ac)), client)
  }

  class DummyJobTimeSpan(jobID: Long, optimumNumExecs: Int) extends JobTimeSpan(jobID) {
    override def optimumNumExecutorsForJob(coresPerExecutor: Int, maxConcurrent: Int): Int = {
      optimumNumExecs
    }
  }

}
