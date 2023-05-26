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

package com.qubole.sparklens.common

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import java.util.Locale
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

import scala.collection.mutable

/*
Keeps track of min max sum mean and variance for any metric at any level
Reference to incremental updates of variance:
https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_Online_algorithm
 */

class AggregateValue {
  var value:    Long   = 0L
  var min:      Long   = Long.MaxValue
  var max:      Long   = Long.MinValue
  var mean:     Double = 0.0
  var variance: Double = 0.0
  var m2:       Double = 0.0

  override def toString(): String = {
    s"""{
       | "value": ${value},
       | "min": ${min},
       | "max": ${max},
       | "mean": ${mean},
       | "m2": ${m2}
       | "variance": ${variance}
       }""".stripMargin
  }

  def getMap(): Map[String, Any] = {
    Map("value" -> value,
    "min" -> min,
    "max" -> max,
    "mean" -> mean,
    "m2" -> m2,
    "variance" -> variance)
  }
}

object AggregateValue {
  def getValue(json: JValue): AggregateValue = {
    implicit val formats = DefaultFormats

    val value = new AggregateValue
    value.value = (json  \ "value").extract[Long]
    value.min = (json \ "min").extract[Long]
    value.max = (json \ "max").extract[Long]
    value.mean = (json \ "mean").extract[Double]
    value.variance = (json \ "variance").extract[Double]
    //making it optional for backward compatibility with sparklens.json files
    value.m2 = (json \ "m2").extractOrElse[Double](0.0)
    value
  }
}

class AggregateMetrics() {
  var count = 0L
  val map = new mutable.HashMap[AggregateMetrics.Metric, AggregateValue]()
  @transient val formatterMap = new mutable.HashMap[AggregateMetrics.Metric, ((AggregateMetrics
  .Metric, AggregateValue), mutable.StringBuilder) => Unit]()
  formatterMap(AggregateMetrics.shuffleWriteTime) = formatNanoTime
  formatterMap(AggregateMetrics.shuffleWriteBytesWritten) = formatBytes
  formatterMap(AggregateMetrics.shuffleWriteRecordsWritten) = formatRecords
  formatterMap(AggregateMetrics.shuffleReadFetchWaitTime) = formatNanoTime
  formatterMap(AggregateMetrics.shuffleReadBytesRead) = formatBytes
  formatterMap(AggregateMetrics.shuffleReadRecordsRead) = formatRecords
  formatterMap(AggregateMetrics.shuffleReadLocalBlocks)= formatRecords
  formatterMap(AggregateMetrics.shuffleReadRemoteBlocks) = formatRecords
  formatterMap(AggregateMetrics.executorRuntime) = formatMillisTime
  formatterMap(AggregateMetrics.jvmGCTime) = formatMillisTime
  formatterMap(AggregateMetrics.executorCpuTime)= formatNanoTime
  formatterMap(AggregateMetrics.resultSize)= formatBytes
  formatterMap(AggregateMetrics.inputBytesRead)= formatBytes
  formatterMap(AggregateMetrics.outputBytesWritten)= formatBytes
  formatterMap(AggregateMetrics.memoryBytesSpilled)= formatBytes
  formatterMap(AggregateMetrics.diskBytesSpilled)= formatBytes
  formatterMap(AggregateMetrics.peakExecutionMemory)= formatBytes
  formatterMap(AggregateMetrics.taskDuration)= formatMillisTime

  @transient val numberFormatter = java.text.NumberFormat.getIntegerInstance

  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (Math.abs(size) >= 1*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (Math.abs(size) >= 1*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (Math.abs(size) >= 1*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else {
        (size.asInstanceOf[Double] / KB, "KB")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  def toMillis(size:Long): String = {
    val MS  = 1000000L
    val SEC = 1000 * MS
    val MT  = 60 * SEC
    val HR  = 60 * MT

    val (value, unit) = {
      if (size >= 1*HR) {
        (size.asInstanceOf[Double] / HR, "hh")
      } else if (size >= 1*MT) {
        (size.asInstanceOf[Double] / MT, "mm")
      } else if (size >= 1*SEC) {
        (size.asInstanceOf[Double] / SEC, "ss")
      } else {
        (size.asInstanceOf[Double] / MS, "ms")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  def formatNanoTime(x: (AggregateMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    sb.append(f" ${x._1}%-30s${toMillis(x._2.value)}%20s${toMillis(x._2.min)}%15s${toMillis(x._2.max)}%15s${toMillis(x._2.mean.toLong)}%20s")
      .append("\n")
  }

  def formatMillisTime(x: (AggregateMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    def addUnits(x: Long): String = {
      toMillis(x * 1000000)
    }
    sb.append(f" ${x._1}%-30s${addUnits(x._2.value)}%20s${addUnits(x._2.min)}%15s${addUnits(x._2.max)}%15s${addUnits(x._2.mean.toLong)}%20s")
      .append("\n")
  }

  def formatBytes(x: (AggregateMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    sb.append(f" ${x._1}%-30s${bytesToString(x._2.value)}%20s${bytesToString(x._2.min)}%15s${bytesToString(x._2.max)}%15s${bytesToString(x._2.mean.toLong)}%20s")
      .append("\n")
  }

  def formatRecords(x: (AggregateMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    sb.append(f" ${x._1}%-30s${numberFormatter.format(x._2.value)}%20s${numberFormatter.format(x._2.min)}%15s${numberFormatter.format(x._2.max)}%15s${numberFormatter.format(x._2.mean.toLong)}%20s")
      .append("\n")
  }

  def updateMetric(metric: AggregateMetrics.Metric, newValue: Long) : Unit = {
    val aggregateValue = map.getOrElse(metric, new AggregateValue)
    if (count == 0) {
      map(metric) = aggregateValue
    }
    aggregateValue.value +=  newValue
    aggregateValue.max    = math.max(aggregateValue.max, newValue)
    aggregateValue.min    = math.min(aggregateValue.min, newValue)
    val delta: Double     = newValue - aggregateValue.mean
    aggregateValue.mean  += delta/(count+1)
    aggregateValue.m2 += delta * (newValue - aggregateValue.mean)
    aggregateValue.variance = aggregateValue.m2 / (count+1)
  }

  def update(tm: TaskMetrics, ti: TaskInfo): Unit = {
    updateMetric(AggregateMetrics.shuffleWriteTime,         tm.shuffleWriteMetrics.writeTime)    //Nano to Millis
    updateMetric(AggregateMetrics.shuffleWriteBytesWritten, tm.shuffleWriteMetrics.bytesWritten)
    updateMetric(AggregateMetrics.shuffleWriteRecordsWritten, tm.shuffleWriteMetrics.recordsWritten)
    updateMetric(AggregateMetrics.shuffleReadFetchWaitTime, tm.shuffleReadMetrics.fetchWaitTime)    //Nano to Millis
    updateMetric(AggregateMetrics.shuffleReadBytesRead,     tm.shuffleReadMetrics.totalBytesRead)
    updateMetric(AggregateMetrics.shuffleReadRecordsRead,   tm.shuffleReadMetrics.recordsRead)
    updateMetric(AggregateMetrics.shuffleReadLocalBlocks,   tm.shuffleReadMetrics.localBlocksFetched)
    updateMetric(AggregateMetrics.shuffleReadRemoteBlocks,  tm.shuffleReadMetrics.remoteBlocksFetched)
    updateMetric(AggregateMetrics.executorRuntime,          tm.executorRunTime)
    updateMetric(AggregateMetrics.jvmGCTime,                tm.jvmGCTime)
    //updateMetric(AggregateMetrics.executorCpuTime,          tm.executorCpuTime) //Nano to Millis
    updateMetric(AggregateMetrics.resultSize,               tm.resultSize)
    updateMetric(AggregateMetrics.inputBytesRead,           tm.inputMetrics.bytesRead)
    updateMetric(AggregateMetrics.outputBytesWritten,       tm.outputMetrics.bytesWritten)
    updateMetric(AggregateMetrics.memoryBytesSpilled,       tm.memoryBytesSpilled)
    updateMetric(AggregateMetrics.diskBytesSpilled,         tm.diskBytesSpilled)
    updateMetric(AggregateMetrics.peakExecutionMemory,      tm.peakExecutionMemory)
    updateMetric(AggregateMetrics.taskDuration,             ti.duration)
    count += 1
  }

  def print(caption: String, sb: mutable.StringBuilder):Unit = {
 sb.append(s" AggregateMetrics (${caption}) total measurements ${count} ")
      .append("\n")
    sb.append(f"                NAME                        SUM                MIN           MAX                MEAN         ")
      .append("\n")
    map.toBuffer.sortWith((a, b) => a._1.toString < b._1.toString).foreach(x => {
      formatterMap(x._1)(x, sb)
    })
  }

  def getMap(): Map[String, Any] = {
    Map("count" -> count, "map" -> map.keys.map(key => (key.toString, map.get(key).get.getMap())).toMap)
  }
}

object AggregateMetrics extends Enumeration {
  import org.json4s._

  type Metric = Value
  val shuffleWriteTime,
  shuffleWriteBytesWritten,
  shuffleWriteRecordsWritten,
  shuffleReadFetchWaitTime,
  shuffleReadBytesRead,
  shuffleReadRecordsRead,
  shuffleReadLocalBlocks,
  shuffleReadRemoteBlocks,
  executorRuntime,
  jvmGCTime,
  executorCpuTime,
  resultSize,
  inputBytesRead,
  outputBytesWritten,
  memoryBytesSpilled,
  diskBytesSpilled,
  peakExecutionMemory,
  taskDuration
  = Value

  def getAggregateMetrics(json: JValue): AggregateMetrics = {
    implicit val formats = DefaultFormats

    val metrics = new AggregateMetrics()
    metrics.count = (json \ "count").extract[Int]
    val map = (json \ "map").extract[Map[String, JValue]]

    map.keys.foreach(key => metrics.map.put(withName(key),
      AggregateValue.getValue(map.get(key).get)))

    metrics
  }

}
