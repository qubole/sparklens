package com.qubole.sparklens

import scala.io.Source

object TestUtils {
  def getFileContents(fileName: String): String = {
    val bufferedSource = Source.fromFile(fileName)
    val result = bufferedSource.mkString
    bufferedSource.close
    result
  }

}
