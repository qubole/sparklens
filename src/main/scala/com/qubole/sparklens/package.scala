package com.qubole

import org.apache.spark.SparkConf

package object sparklens {

  private [qubole] def getDumpDirectory(conf: SparkConf): String = {
    conf.get("spark.sparklens.dump.dir", "/tmp/sparklens/")
  }

  private [qubole] def asyncReportingEnabled(conf: SparkConf): Boolean = {
    // This will dump info to `getDumpDirectory()` and not run reporting
    conf.getBoolean("spark.sparklens.simulation.async", false)
  }

  private [qubole] def dumpDataEnabled(conf: SparkConf): Boolean = {
    /* Even if reporting is in app, we can still dump sparklens data which could be used later */
    conf.getBoolean("spark.sparklens.dump.data", true)
  }
}
