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

    assert (outputFromSparklensDump(sparklensDump) ==
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

}
