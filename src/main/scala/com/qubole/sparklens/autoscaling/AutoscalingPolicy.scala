package com.qubole.sparklens.autoscaling

import com.qubole.sparklens
import com.qubole.sparklens.app.ReporterApp
import com.qubole.sparklens.common.AppContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.autoscaling.AutoscalingSparklensClient
import org.apache.spark.externalautoscaling.ExecutorAllocator
import org.apache.spark.scheduler.{SparkListenerExecutorAdded, SparkListenerExecutorRemoved, SparkListenerJobEnd, SparkListenerJobStart}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.LoggerFactory

import scala.collection.mutable
class AutoscalingPolicy(autoscalingClient: AutoscalingSparklensClient, sparkConf: SparkConf,
                        unitTestAppContext: Option[AppContext] = None) {

  import AutoscalingPolicy.log
  private val map = init

  // always taking lock on currentExecutors for any action on add/remove/request executor
  private val currentExecutors: mutable.Set[String] =  new mutable.HashSet[String]()
  private val execsToBeReleased: mutable.Set[String] =  new mutable.HashSet[String]()

  @volatile private var lastNumExecutorsRequested: Int =
    sparkConf.getInt("spark.dynamicAllocation.minExecutors",
    sparkConf.getInt("spark.executor.instances", 0))


  // maybe this needs to be delayed till SparkContext has come up
  private def init: Map[ExecutorChange, Int] = {
    log.debug("Init for sparklens autoscaling policy")
    val olderAppContext = unitTestAppContext match {
      case Some(context) => context
      case None => getAppContext(sparkConf) match {
        case Some(appContext) =>
          log.debug(s"Got appContext for previous run ${appContext}")
          appContext
        case None =>
          log.warn("Could not find any previous appContext for sparklens autoscaling.")
          return Map.empty
      }
    }

    val coresPerExecutor = olderAppContext.executorMap.values.map(x => x.cores).sum /
      olderAppContext.executorMap.size
    createAutoscaleExec(olderAppContext, coresPerExecutor)
  }

  private def getAppContext(conf: SparkConf): Option[AppContext] = {
    sparklens.sparklensPreviousDump(conf) match {
      case Some(file) =>
        val json = ReporterApp.readSparklenDump(file)
        implicit val formats = DefaultFormats
        val map = parse(json).extract[JValue]
        Some(AppContext.getContext(map))
      case _ => None
    }
  }

  private def createAutoscaleExec(ac: AppContext, coresPerExecutor: Int): Map[ExecutorChange, Int]
  = {
    log.debug("Creating autoscaling policy based on previous run")
    val maxConcurrentExecutors = AppContext.getMaxConcurrent(ac.executorMap, ac)

    var lastJobEndTime = ac.appInfo.startTime
    var lastJobID = -1
    val map = new mutable.HashMap[ExecutorChange, Int]


    ac.jobMap.values.toSeq.sortWith(_.startTime < _.startTime).foreach(jobTimeSpan => {
      val optimalExecutors = jobTimeSpan.optimumNumExecutorsForJob(coresPerExecutor,
        maxConcurrentExecutors.asInstanceOf[Int])

      if (jobTimeSpan.startTime - lastJobEndTime > AutoscalingPolicy.releaseTimeout) {
        log.debug(s"Exec: job: ${lastJobID} end, num = 0")
        map.put(ExecutorChange(lastJobID, false), 0)
      }

      log.debug(s"Exec: job: ${jobTimeSpan.jobID} start, num = ${optimalExecutors}")
      map.put(ExecutorChange(jobTimeSpan.jobID.toInt, true), optimalExecutors)
      lastJobEndTime = jobTimeSpan.endTime
      lastJobID = jobTimeSpan.jobID.toInt
    })
    log.debug(s"Exec: job: ${lastJobID} end, num = 0")

    map.put(ExecutorChange(lastJobID, false), 0)
    map.toMap
  }

  def scale(jobStart: SparkListenerJobStart): Unit = {
    scale(jobStart.jobId, true)
  }

  def scale(jobEnd: SparkListenerJobEnd): Unit = {
    scale(jobEnd.jobId, false)
  }

  def addExecutor(executorAdded: SparkListenerExecutorAdded): Unit = {
    log.debug(s"Adding executor ${executorAdded.executorId}")
    currentExecutors.synchronized {
      currentExecutors.add(executorAdded.executorId)
    }
    // a new executor could come due to an old request which might not be needed now. So
    // try to remove extra executors
    scale(lastNumExecutorsRequested)
  }

  def removeExecutor(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    currentExecutors.synchronized {
      if (!currentExecutors.contains(executorRemoved.executorId)) {
        log.warn(s"Strange that currentExecutors does not have ${executorRemoved.executorId}.")
      }
      currentExecutors.remove(executorRemoved.executorId)
      if (!execsToBeReleased.contains(executorRemoved.executorId)) {
        log.warn("Exec to be release did not contain the executor. Someone else asked for " +
          s"removal of this executor ${executorRemoved.executorId}")
      }
      log.debug(s"removing executor ${executorRemoved.executorId}")
      execsToBeReleased.remove(executorRemoved.executorId)
    }
  }

  private def scale(jobId: Int, jobStart: Boolean): Unit = {
    log.debug(s"scale called for jobId = ${jobId}")
    map.get(ExecutorChange(jobId, jobStart)) match {
      case Some(numExecs) => scale(numExecs)
      case _ => log.debug("map was empty while trying to scale")
    }
  }

  private def scale(numExecs: Int): Unit = {
    log.debug(s"Last executors requested = ${lastNumExecutorsRequested}, and current request = " +
      s"${numExecs}")
    if (lastNumExecutorsRequested >= numExecs) {
      currentExecutors.synchronized {
        val toRemove = (currentExecutors.size - execsToBeReleased.size) - numExecs
        val execsToRemove = (currentExecutors -- execsToBeReleased).take(toRemove).toSeq
        log.debug(
          s"""Current executors = ${currentExecutors}. Size = ${currentExecutors.size}
             | Executors waiting to be released = ${execsToBeReleased}. Size =
             | ${execsToBeReleased.size}.
             | Number of executors to be removed now = ${toRemove}.
             | Executors chosen to be removed = ${execsToRemove}
           """.stripMargin)
        execsToRemove.foreach(execsToBeReleased.add(_))
        if (!execsToRemove.isEmpty) autoscalingClient.killExecutors(execsToRemove)
      }
    }
    if (lastNumExecutorsRequested != numExecs) {
      log.info(s"Asking autosclaling client to scale to ${numExecs} from previous " +
        s"${lastNumExecutorsRequested}")
      lastNumExecutorsRequested = numExecs
      autoscalingClient.requestTotalExecutors(numExecs)
    }
  }
}

object AutoscalingPolicy {
  val releaseTimeout = 2 * 60 * 1000  // time between 2 jobs to release all resources
  val log = LoggerFactory.getLogger(classOf[AutoscalingPolicy])

  def init(sparkConf: SparkConf): Option[AutoscalingSparklensClient] = {
    val spark = try {
      SparkContext.getOrCreate(sparkConf)
    } catch { // while replaying from event-history, SparkContext would not be present.
      case _: Throwable => return None
    }

    val klass = spark.getClass

    // this relection will be removed once OSS spark has pluggable autoscaling
    val executorAllocatorMethod = try {
      klass.getMethod("executorAllocator")
    } catch {
      case ne: NoSuchMethodException =>
        log.warn("Could not get executorAllocator from SparkContext. Will not be able to perform " +
          "sparklens autoscaling")
        return None
    }
    executorAllocatorMethod.invoke(spark).asInstanceOf[Option[ExecutorAllocator]] match {
      case Some(client) if client.isInstanceOf[AutoscalingSparklensClient] =>
        Some(client.asInstanceOf[AutoscalingSparklensClient])
      case _ =>
        log.warn("Could not cast ExecutorAllocator to AutoscalingSparklensClient. Will not be " +
          "able to perform sparklens autoscaling.")
        None
    }
  }
}
