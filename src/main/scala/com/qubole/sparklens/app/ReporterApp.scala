package com.qubole.sparklens.app

import java.net.URI

import com.google.gson.{Gson, JsonObject}
import com.qubole.sparklens.analyzer.AppAnalyzer
import com.qubole.sparklens.app.ReporterApp.json
import com.qubole.sparklens.common.{AppContext, ApplicationInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object ReporterApp extends App {
  checkArgs()

  val file = args(0)
  val fs = FileSystem.get(new URI(file), new Configuration())

  val path = new Path(file)
  val byteArray = new Array[Byte](fs.getFileStatus(path).getLen.toInt)
  fs.open(path).readFully(byteArray)

  val json = (byteArray.map(_.toChar)).mkString
  startAnalysersFromString(json)

  private def checkArgs(): Unit = {
    args.size match {
      case x if x < 1 => throw new IllegalArgumentException("Need to specify sparklens data file")
      case _ => // Do nothing
    }
  }

  def startAnalysersFromString(json: String): Unit = {
    val map = new Gson().fromJson(json, classOf[JsonObject])

    val appContext = AppContext.getContext(map)
    AppAnalyzer.startAnalyzers(appContext)
  }
}