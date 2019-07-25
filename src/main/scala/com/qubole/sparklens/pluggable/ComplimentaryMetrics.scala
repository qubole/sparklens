package com.qubole.sparklens.pluggable

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart}
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.JsonAST.JValue

import scala.collection.mutable

trait ComplimentaryMetrics {
  def getMap(): Map[String, _ <: Any] = {
    throw new NotImplementedError(s"getMap() method is not implemented.")
  }

  def getObject(json: JValue): ComplimentaryMetrics = {
    throw new NotImplementedError(s"getObject() method is not implemented.")
  }

  def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    throw new NotImplementedError(s"getObject() method is not implemented.")
  }

  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    throw new NotImplementedError(s"getObject() method is not implemented.")
  }

  def print(caption: String, sb: mutable.StringBuilder): Unit = {
    throw new NotImplementedError(s"getObject() method is not implemented.")
  }
}

object ComplimentaryMetrics {

  /**
    * Returns object which extends [[ComplimentaryMetrics]] by matching input string
    */
  def fromString(value: String): ComplimentaryMetrics = {
    value.toLowerCase match {
      case "drivermetrics" => DriverMetrics
      case _ => throw new Exception(s"Object ${value} not found.")
    }
  }

  /**
    * Used for for extracting the pluggableMetricsMap in [[com.qubole.sparklens.QuboleJobListener]]
    * to construct [[com.qubole.sparklens.common.AppContext]] from the JSON.
    */
  def getMetricsMap(json: Map[String, JValue]): mutable.HashMap[String, ComplimentaryMetrics] = {
    val metricsMap = new mutable.HashMap[String, ComplimentaryMetrics]
    try {
      implicit val formats = DefaultFormats
      val metricsMap = new mutable.HashMap[String, ComplimentaryMetrics]
      json.keys.map(key => {
        val value = json.get(key).get
        metricsMap.put(key, fromString(key).getObject(value))
      })
    } catch {
      case e: Exception if !e.isInstanceOf[MappingException] =>
        throw(e)
    }
    metricsMap
  }

  /**
    * Used for for converting the pluggableMetricsMap in [[com.qubole.sparklens.QuboleJobListener]]
    * to a formatted map which is then dumped in the JSON file/printed on console.
    */
  def getMap(metricsMap: mutable.HashMap[String, _ <: ComplimentaryMetrics]): Map[String, Any] = {
    metricsMap.keys.map(key => (key.toString, metricsMap(key).getMap)).toMap
  }
}