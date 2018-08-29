package com.qubole

import org.apache.spark.SparkConf

package object sparklens {

  private [qubole] def getDumpDirectory(conf: SparkConf): String = {
    conf.get("spark.sparklens.data.dir", "/tmp/sparklens/")
  }

  private [qubole] def asyncReportingEnabled(conf: SparkConf): Boolean = {
    // This will dump info to `getDumpDirectory()` and not run reporting
    conf.getBoolean("spark.sparklens.reporting.disabled", false)
  }

  private [qubole] def dumpDataEnabled(conf: SparkConf): Boolean = {
    /* Even if reporting is in app, we can still dump sparklens data which could be used later */
    conf.getBoolean("spark.sparklens.save.data", true)
  }
  def sparklensPreviousDump(conf: SparkConf): Option[String] = {
    conf.getOption("spark.sparklens.previous.data.file")
  }

  // Width and length of autoscale graph
  private[qubole] def autoscaleGraphDimensions(conf: SparkConf): List[Int] = {
    conf.getOption("spark.sparklens.autoscale.graph.dimensions") match {
      case Some(dimensions) => dimensions.split("x").toList.map(_.toInt)
      case _ => List(100, 30)
    }
  }
}
