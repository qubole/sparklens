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

import java.util.Locale

import com.google.gson.{Gson, JsonObject}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import scala.collection.mutable

/*
Keeps track of min max sum mean and variance for any metric at any level
The mean and variance code was picked up from another spark listener
 */

class AggregateValue {
  var value:    Long   = 0L
  var min:      Long   = Long.MaxValue
  var max:      Long   = Long.MinValue
  var mean:     Double = 0.0
  var variance: Double = 0.0

  override def toString(): String = {
    s"""{
       | "value": ${value},
       | "min": ${min},
       | "max": ${max},
       | "mean": ${mean},
       | "variance": ${variance}
       }""".stripMargin
  }
}

object AggregateValue {
  def getValue(json: JsonObject): AggregateValue = {
    val value = new AggregateValue
    value.value = json.get("value").getAsLong
    value.min = json.get("min").getAsLong
    value.max = json.get("max").getAsLong
    value.mean = json.get("mean").getAsDouble
    value.variance = json.get("variance").getAsDouble
    value
  }
}

class AggregateMetrics() {
  var count = 0L
  val map = new mutable.HashMap[AggregateMetrics.Metric, AggregateValue]()
  @transient val formatterMap = new mutable.HashMap[AggregateMetrics.Metric, ((AggregateMetrics
  .Metric, AggregateValue), mutable.StringBuilder) => Unit]()
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
    aggregateValue.variance += delta * (newValue - aggregateValue.mean)
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

  override def toString(): String = {
    getJavaMap.toString
  }

  def getJavaMap():java.util.Map[String, Any] = {
    import scala.collection.JavaConverters._

    Map("count" -> count, "map" -> map.asJava).asJava
  }
}

object AggregateMetrics extends Enumeration {

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

  def getAggregateMetrics(json: JsonObject): AggregateMetrics = {
    val metrics = new AggregateMetrics()
    metrics.count = json.get("count").getAsInt
    val map = json.get("map").getAsJsonObject
    import scala.collection.JavaConverters._

    for (elem <- map.entrySet().asScala) {
      metrics.map.put(withName(elem.getKey), AggregateValue.getValue(elem.getValue.getAsJsonObject))
    }
    metrics
  }

}



