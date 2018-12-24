package org.apache.spark.autoscaling

import org.apache.spark.ExecutorAllocationClient

import scala.collection.mutable

class DummyAutoscalingSparklensClient(client: ExecutorAllocationClient)
  extends AutoscalingSparklensClient(client) {

  val updateRequests = mutable.ArrayBuffer.empty[Int]
  val killRequests = mutable.ArrayBuffer.empty[Seq[String]]

  override def requestTotalExecutors(num: Int): Unit = {
    updateRequests.append(num)
  }

  override def killExecutors(execs: Seq[String]): Unit = {
    killRequests.append(execs)
  }

}
