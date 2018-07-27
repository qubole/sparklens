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
    /* When asyncReporting is dis-abled, we are not dumping data right now
     * to maintain status quo. However, this can be changed later to always dumping.
     */
    conf.getBoolean("spark.sparklens.dump.data", true)
  }
}
