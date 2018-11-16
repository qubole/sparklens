package com.qubole.sparklens.autoscaling

import java.util.concurrent.atomic.AtomicBoolean

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

  @volatile private var initalized: AtomicBoolean = new AtomicBoolean(false)
  @volatile private var lastNumExecutorsRequested: Int =
    getDynamicAllocationInitialExecutors(sparkConf)


  /** The following functions getDynamicAllocationInitialExecutors,
   *                         getDynamicAllocationMinExecutors and
   *                         getDynamicAllocationMaxExecutors
   *  have been copied from Qubole Spark repo's Utils.scala
   */

  /**
    * Return the initial number of executors for dynamic allocation.
    */
  def getDynamicAllocationInitialExecutors(conf: SparkConf): Int = {
    conf.getInt("spark.dynamicAllocation.initialExecutors",
      getDynamicAllocationMinExecutors(conf))
  }

  /**
    * Return the minimum number of executors for dynamic allocation.
    */
  def getDynamicAllocationMinExecutors(conf: SparkConf): Int = {
    conf.getInt("spark.dynamicAllocation.minExecutors",
      conf.getInt("spark.executor.instances", 0))
  }

  /**
    * Return the maximum number of executors for dynamic allocation.
    */
  def getDynamicAllocationMaxExecutors(conf: SparkConf): Int = {
    val defaultMaxExecutors = conf.getInt("spark.qubole.internal.default.maxExecutors",
      2)
    val maxExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors",
      conf.getInt("spark.qubole.max.executors", defaultMaxExecutors))
    maxExecutors.max(getDynamicAllocationMinExecutors(conf))
  }


  // maybe this needs to be delayed till SparkContext has come up
  private def init: Map[Int, Int] = {
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

  private def createAutoscaleExec(ac: AppContext, coresPerExecutor: Int): Map[Int, Int] = {
    log.debug("Creating autoscaling policy based on previous run")
    val maxConcurrentExecutors = AppContext.getMaxConcurrent(ac.executorMap, ac)

    ac.jobMap.values.map(jobTimeSpan => {
      val optimalExecutors = jobTimeSpan.optimumNumExecutorsForJob(coresPerExecutor,
        maxConcurrentExecutors.asInstanceOf[Int])
      log.debug(s"Executors required for job = ${jobTimeSpan.jobID} = ${optimalExecutors}")
      jobTimeSpan.jobID.asInstanceOf[Int] -> optimalExecutors
    }).toMap
  }

  def scale(jobStart: SparkListenerJobStart): Unit = {
    scale(jobStart.jobId, true)
  }

  def scale(jobEnd: SparkListenerJobEnd): Unit = {
    scale(jobEnd.jobId, false)
  }

  def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    log.debug(s"Adding executor ${executorAdded.executorId}")
    currentExecutors.synchronized {
      currentExecutors.add(executorAdded.executorId)
    }
    // a new executor could come due to an old request which might not be needed now. So
    // try to remove extra executors
    scale(lastNumExecutorsRequested)
  }

  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
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
    log.debug(s"scale called for jobId = ${jobId}, job started = ${jobStart}")
    map.get(jobId) match {
      case Some(numExecs) =>
        currentExecutors.synchronized {
          jobStart match {
            case true =>
              initalized.getAndSet(true) match { // initial num of  executors
                case false =>
                  log.debug(s"Reset: Either starting, or the last job completed was more than " +
                    s"${AutoscalingPolicy.releaseTimeout}ms before. Scaling to ${numExecs} " +
                    s"executors specifically for job: ${jobId}")
                  scale(numExecs)
                case true =>
                  log.debug(s"Adding ${numExecs} for job ${jobId} to already existing " +
                    s"${lastNumExecutorsRequested}")
                  scale(lastNumExecutorsRequested + numExecs)
              }
            case false =>
              (lastNumExecutorsRequested - numExecs) match {
                case 0 => // downscale only after release timeout
                  log.debug(s"Job ${jobId} complete, will scale down to 0 only after " +
                    s"${AutoscalingPolicy.releaseTimeout}ms if no other jobs would come in that " +
                    s"time.")
                  initalized.getAndSet(false)
                  new Thread(new Runnable {
                    override def run(): Unit = {
                      Thread.sleep(AutoscalingPolicy.releaseTimeout)
                      currentExecutors.synchronized {
                        if (initalized.get() == false) {
                          if (lastNumExecutorsRequested == numExecs) scale(0) // Still no jobs have come
                        }
                      }
                    }
                  }).start()
                case _ =>
                  log.debug(s"downscaling by ${numExecs} from current asked " +
                    s"${lastNumExecutorsRequested} for job ${jobId}")
                  scale(lastNumExecutorsRequested - numExecs) // parallel jobs, downscale
              }
          }
        }
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

      // upscale only to a max number
      val finalNum = Math.min(numExecs, getDynamicAllocationMaxExecutors(sparkConf))
      if (finalNum < numExecs) {
        log.info(s"Although ${numExecs} number of executors requested, but limiting the request " +
          s"to a maximum of ${finalNum} configured")
      }
      autoscalingClient.requestTotalExecutors(finalNum)
    }
  }
}

object AutoscalingPolicy {
  var releaseTimeout = 2 * 60 * 1000  // time between 2 jobs to release all resources
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
   def setTimeoutForUnitTest(timeout: Int): Unit = {
     releaseTimeout = timeout
   }
}
