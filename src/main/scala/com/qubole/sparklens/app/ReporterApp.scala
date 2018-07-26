package com.qubole.sparklens.app

import java.net.URI

import com.qubole.sparklens.analyzer.AppAnalyzer
import com.qubole.sparklens.common.AppContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse

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

    implicit val formats = DefaultFormats
    val map = parse(json).extract[JValue]

    val appContext = AppContext.getContext(map)
    AppAnalyzer.startAnalyzers(appContext)
  }
}