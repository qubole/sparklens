package com.qubole.sparklens.helper

import java.io.File
import java.io.FileWriter

import com.mashape.unirest.http.Unirest
import org.apache.spark.SparkConf

object EmailReportHelper {

  def getTempFileLocation(): String = {
    val random = new scala.util.Random(31)
    s"/tmp/sparklens/${random.nextInt.toString}.json"
  }

  def isValid(email: String): Boolean =
    """(\w+)@([\w\.]+)""".r.unapplySeq(email).isDefined

  def generateReport(appContextString: String, conf: SparkConf): Unit = {
    Option(conf.get("spark.sparklens.report.email", null)) match {
      case Some(email) =>
        if (!isValid(email)) {
          println(s"Email $email is not valid. Please provide a valid email.")
          return
        }
        val tempFileLocation = getTempFileLocation()
        try {
          val fileWriter = new FileWriter(tempFileLocation)
          fileWriter.write(appContextString)
          fileWriter.close()
          val response = Unirest.post("http://sparklens.qubole.com/generate_report/request_generate_report")
            .field("file-2[]", new File(tempFileLocation))
            .field("email", email)
            .asJson()
          println(response.getBody)
        } catch {
          case e: Exception =>
            println(e.getMessage)
        } finally {
          val file = new File(tempFileLocation)
          file.deleteOnExit()
        }
      case _ =>
    }
  }
}
