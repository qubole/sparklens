package com.qubole.sparklens.pluggable
import org.json4s.JsonAST

class SQLMetrics extends ComplimentaryMetrics {
  override def getMap(): Map[String, _] = {
    null
  }
}

object SQLMetrics extends ComplimentaryMetrics {
  override def getObject(json: JsonAST.JValue): ComplimentaryMetrics = {
    null
  }
}

