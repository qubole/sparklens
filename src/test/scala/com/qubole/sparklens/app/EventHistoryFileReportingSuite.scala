package com.qubole.sparklens.app

import java.io.{ByteArrayOutputStream, PrintStream}

import com.qubole.sparklens.TestUtils
import org.scalatest.FunSuite

class EventHistoryFileReportingSuite extends FunSuite {

  test("Reporting from sparklens and event-history should be same") {
    val eventHistoryFile = s"${System.getProperty("user.dir")}" +
      s"/src/test/event-history-test-files/local-1532512550423"

    // corresponding sparklens dump is in same location and name, but additional suffix
    val sparklensDump = TestUtils.getFileContents(eventHistoryFile + ".sparklens.json")

    validateOutput(outputFromSparklensDump(sparklensDump),
      outputFromEventHistoryReport(eventHistoryFile))
  }


  private def outputFromSparklensDump(dump: String): String = {
    val out = new ByteArrayOutputStream()
    Console.withOut(new PrintStream(out)) {
      ReporterApp.startAnalysersFromString(dump)
    }
    out.toString

  }
  private def outputFromEventHistoryReport(file: String): String = {
    val out = new ByteArrayOutputStream()
    Console.withOut(new PrintStream(out)) {
      new EventHistoryReporter(file)
    }
    out.toString
  }

  private def validateOutput(file1:String, file2:String) = {
    assert(file1.size == file2.size,
      "output size is different between eventlogs report and sparklens.json report")
    assert(file1.lines.zip(file2.lines).filterNot(x => x._1 == x._2).size == 0,
      "Report lines are not matching between eventlogs report and sparklens.json report")
  }
}
