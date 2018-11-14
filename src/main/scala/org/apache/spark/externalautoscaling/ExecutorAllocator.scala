package org.apache.spark.externalautoscaling

trait ExecutorAllocator {

  def start(): Unit

  def stop(): Unit
}
