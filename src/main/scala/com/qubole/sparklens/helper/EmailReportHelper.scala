package com.qubole.sparklens.helper

import java.io.File

import com.mashape.unirest.http.Unirest
import org.apache.spark.SparkConf

object EmailReportHelper {

  def generateReport(sparklensJsonFile: String, conf: SparkConf): Unit = {
    Option(conf.get("spark.sparklens.report.email")) match {
      case Some(email) =>
        try {
          val response = Unirest.post("http://sparklens.qubole.com/generate_report/request_generate_report")
            .field("file-2[]", new File(sparklensJsonFile))
            .field("email", email)
            .asJson()
        } catch {
          case e: Exception =>
            println(e.getMessage)
        }
      case _ =>
    }
  }
}
