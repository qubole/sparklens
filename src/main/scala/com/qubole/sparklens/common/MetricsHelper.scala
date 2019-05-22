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

package com.qubole.sparklens.common

import java.util.Locale

object MetricsHelper {

  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (Math.abs(size) >= 1*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (Math.abs(size) >= 1*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (Math.abs(size) >= 1*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else {
        (size.asInstanceOf[Double] / KB, "KB")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  def toMillis(size:Long): String = {
    val MS  = 1000000L
    val SEC = 1000 * MS
    val MT  = 60 * SEC
    val HR  = 60 * MT

    val (value, unit) = {
      if (size >= 1*HR) {
        (size.asInstanceOf[Double] / HR, "hh")
      } else if (size >= 1*MT) {
        (size.asInstanceOf[Double] / MT, "mm")
      } else if (size >= 1*SEC) {
        (size.asInstanceOf[Double] / SEC, "ss")
      } else {
        (size.asInstanceOf[Double] / MS, "ms")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }
}