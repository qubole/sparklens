/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.qubole.sparklens.timespan

import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

/*
 * We will look at the application as a sequence of timeSpans
 */
trait TimeSpan  {
  var startTime: Long = 0
  var endTime: Long = 0

  def setEndTime(time: Long): Unit = {
    endTime = time
  }

  def setStartTime(time: Long): Unit = {
    startTime = time
  }
  def isFinished(): Boolean = (endTime != 0 && startTime != 0)

  def duration(): Option[Long] = {
    if (isFinished()) {
      Some(endTime - startTime)
    } else {
      None
    }
  }
  def getMap(): Map[String, _ <: Any]

  def getStartEndTime(): Map[String, Long] = {
    Map("startTime" -> startTime, "endTime" -> endTime)
  }

  def addStartEnd(json: JValue): Unit = {
    implicit val formats = DefaultFormats
    this.startTime = (json \ "startTime").extract[Long]
    this.endTime = (json \ "endTime").extract[Long]
  }
}
