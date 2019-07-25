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

package com.qubole.sparklens.pluggable

import java.lang.management.ManagementFactory
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.qubole.sparklens.common.AggregateValue
import com.qubole.sparklens.common.MetricsHelper._
import javax.management.{Attribute, ObjectName}
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart}
import org.json4s

import scala.collection.mutable

class DriverMetrics extends ComplimentaryMetrics {

  private val ProcessCpuTime = "ProcessCpuTime"

  val map = new mutable.HashMap[DriverMetrics.Metric, AggregateValue]()
  @transient val formatterMap = new mutable.HashMap[DriverMetrics.Metric, ((DriverMetrics
  .Metric, AggregateValue), mutable.StringBuilder) => Unit]()

  formatterMap(DriverMetrics.driverHeapMax) = formatStaticBytes
  formatterMap(DriverMetrics.driverMaxHeapCommitted) = formatStaticBytes
  formatterMap(DriverMetrics.driverMaxHeapUsed) = formatStaticBytes
  formatterMap(DriverMetrics.driverCPUTime) = formatStaticMillisTime
  formatterMap(DriverMetrics.driverGCTime) = formatStaticMillisTime
  formatterMap(DriverMetrics.driverGCCount) = formatCount


  private val threadExecutor = Executors.newSingleThreadScheduledExecutor
  threadExecutor

  val updateDriverMemMetrics = new Runnable {
    def run() = {
      val memUsage = java.lang.management.ManagementFactory.getMemoryMXBean.getHeapMemoryUsage
      updateMetric(DriverMetrics.driverHeapMax, memUsage.getMax)
      updateMetric(DriverMetrics.driverMaxHeapCommitted, memUsage.getCommitted)
      updateMetric(DriverMetrics.driverMaxHeapUsed, memUsage.getUsed)
    }
  }

  def collectGCMetrics(): Unit = {
    val operatingSystemObjectName = ObjectName.getInstance("java.lang:type=OperatingSystem")
    updateMetric(DriverMetrics.driverCPUTime,
      ManagementFactory.getPlatformMBeanServer
        .getAttribute(operatingSystemObjectName, ProcessCpuTime).asInstanceOf[Attribute]
        .getValue.asInstanceOf[Long])

    var gcCount: Long = 0
    var gcTime: Long = 0
    val iter = ManagementFactory.getGarbageCollectorMXBeans.iterator()
    while (iter.hasNext) {
      val current = iter.next()
      gcCount += current.getCollectionCount
      gcTime += current.getCollectionTime
    }
    updateMetric(DriverMetrics.driverGCTime, gcTime)
    updateMetric(DriverMetrics.driverGCCount, gcCount)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    scheduleMetricsCollection()
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    collectGCMetrics()
    terminateMetricsCollection()
  }

  // Start a thread to collect the driver JVM memory stats every 10 seconds
  def scheduleMetricsCollection(): Unit = {
    threadExecutor.scheduleAtFixedRate(updateDriverMemMetrics, 0, 10, TimeUnit.SECONDS)
  }

  def terminateMetricsCollection(): Unit = {
    threadExecutor.shutdown()
  }

  def updateMetric(metric: DriverMetrics.Metric, newValue: Long): Unit = {
    val aggregateValue = map.getOrElse(metric, new AggregateValue)
    if (!map.contains(metric)) {
      map(metric) = aggregateValue
    }
    aggregateValue.value  = math.max(aggregateValue.max, newValue)
  }

  override def getMap(): Map[String, Any] = {
    Map("map" -> map.keys.map(key => (key.toString, map.get(key).get.getMap())).toMap)
  }

  def formatStaticMillisTime(x: (DriverMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    def addUnits(x: Long): String = {
      toMillis(x * 1000000)
    }
    sb.append(f" ${x._1}%-30s${addUnits(x._2.value)}%20s")
      .append("\n")
  }

  def formatStaticBytes(x: (DriverMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    sb.append(f" ${x._1}%-30s${bytesToString(x._2.value)}%20s")
      .append("\n")
  }

  def formatCount(x: (DriverMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    sb.append(f" ${x._1}%-30s${x._2.value}%20s")
      .append("\n")
  }

  override def print(caption: String, sb: mutable.StringBuilder):Unit = {
    sb.append(s" DriverMetrics (${caption}) ")
      .append("\n")
    sb.append(f"                NAME                        Value         ")
      .append("\n")

    map.toBuffer.sortWith((a, b) => a._1.toString < b._1.toString).foreach(x => {
      formatterMap(x._1)(x, sb)
    })
  }
}

object DriverMetrics extends Enumeration with ComplimentaryMetrics {
  import org.json4s._

  type Metric = Value

  val driverHeapMax,
  driverMaxHeapCommitted,
  driverMaxHeapUsed,
  driverCPUTime,
  driverGCCount,
  driverGCTime
  = Value

  override def getObject(json: json4s.JValue): ComplimentaryMetrics = {
    try {
      implicit val formats = DefaultFormats

      val metrics = new DriverMetrics()
      val map = (json \ "map").extract[Map[String, JValue]]

      map.keys.foreach(key => metrics.map.put(withName(key),
        AggregateValue.getValue(map.get(key).get)))

      metrics
    } catch {
      case e: MappingException =>
        new DriverMetrics()
      case e: Exception =>
        throw(e)
    }
  }
}
