package com.qubole.sparklens.app

import java.io.{BufferedInputStream, InputStream}
import java.net.URI

import com.ning.compress.lzf.LZFInputStream
import com.qubole.sparklens.QuboleJobListener
import net.jpountz.lz4.LZ4BlockInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.xerial.snappy.SnappyInputStream


class EventHistoryReporter(file: String) {

  val busKlass = Class.forName("org.apache.spark.scheduler.ReplayListenerBus")
  val bus = busKlass.newInstance()
  val addListenerMethod = busKlass.getMethod("addListener", classOf[java.lang.Object])
  val conf = new SparkConf()
    .set("spark.sparklens.simulation.async", "false")
    .set("spark.sparklens.dump.data", "false")
  val listener = new QuboleJobListener(conf)
  val replayMethod = busKlass.getMethod("replay", classOf[InputStream], classOf[String],
    classOf[Boolean], classOf[(String) => Boolean])

  addListenerMethod.invoke(bus, listener)
  replayMethod.invoke(bus, getDecodedInputStream(file, conf), file, boolean2Boolean(false),
    getFilter _)

  // Borrowed from CompressionCodecs in spark
  private def getDecodedInputStream(file: String, conf: SparkConf): InputStream = {

    val fs = FileSystem.get(new URI(file), new Configuration())
    val path = new Path(file)
    val bufStream = new BufferedInputStream(fs.open(path))

    val logName = path.getName.stripSuffix(".inprogress")
    val codecName: Option[String] = logName.split("\\.").tail.lastOption

    codecName.getOrElse("") match {
      case "lz4" => new LZ4BlockInputStream(bufStream)
      case "lzf" => new LZFInputStream(bufStream)
      case "snappy" => new SnappyInputStream(bufStream)
      case _ => bufStream
    }
  }

  private def getFilter(eventString: String): Boolean = {
    implicit val formats = DefaultFormats
    eventFilter.contains(parse(eventString).extract[Map[String, Any]].get("Event")
      .get.asInstanceOf[String])
  }

  private def eventFilter: Set[String] = {
    Set(
      "SparkListenerTaskEnd",
      "SparkListenerApplicationStart",
      "SparkListenerApplicationEnd",
      "SparkListenerExecutorAdded",
      "SparkListenerExecutorRemoved",
      "SparkListenerJobStart",
      "SparkListenerJobEnd",
      "SparkListenerStageSubmitted",
      "SparkListenerStageCompleted"
    )
  }

}
