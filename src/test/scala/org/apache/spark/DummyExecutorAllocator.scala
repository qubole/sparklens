package org.apache.spark

class DummyExecutorAllocator extends ExecutorAllocationClient {
  override private[spark] def getExecutorIds() = ???

  override private[spark] def requestTotalExecutors(numExecutors: Int, localityAwareTasks: Int, hostToLocalTaskCount: Map[String, Int]) = ???

  override def requestExecutors(numAdditionalExecutors: Int): Boolean = ???

  override def killExecutors(executorIds: Seq[String]): Boolean = ???
}
