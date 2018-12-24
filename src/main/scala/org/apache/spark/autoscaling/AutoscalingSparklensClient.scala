package org.apache.spark.autoscaling

import org.apache.spark.ExecutorAllocationClient
import org.apache.spark.externalautoscaling.ExecutorAllocator

class AutoscalingSparklensClient(client: ExecutorAllocationClient) extends ExecutorAllocator {

  override def start(): Unit = {}

  override def stop(): Unit = {}

  def requestTotalExecutors(num: Int): Unit = {
    client.requestTotalExecutors(num, 0, Map.empty)
  }

  def killExecutors(execs: Seq[String]): Unit = {
    try {
      client.killExecutors(execs)
    } catch {
      case nsme: NoSuchMethodError => // not spark-2.0.0, using only for spark-2.3
        val method = client.getClass.getMethod("killExecutors", classOf[Seq[String]],
          classOf[Boolean], classOf[Boolean], classOf[Boolean])
        method.invoke(client, execs,
          boolean2Boolean(true), boolean2Boolean(false), boolean2Boolean(true))
    }
  }
}