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

import org.json4s.StringInput

object Json4sWrapper {

  private val parseMethod = {
    try {
      org.json4s.jackson.JsonMethods.getClass.getDeclaredMethod("parse", classOf[org.json4s.JsonInput], classOf[Boolean])
    }catch {
      case ne: NoSuchMethodException =>
        org.json4s.jackson.JsonMethods.getClass.getDeclaredMethod("parse", classOf[org.json4s.JsonInput], classOf[Boolean], classOf[Boolean])
    }
  }

  private val methodsObject = {
    val objectName = "org.json4s.jackson.JsonMethods$"
    val cons = Class.forName(objectName).getDeclaredConstructors();
    cons(0).setAccessible(true);
    val jsonMethodObject:org.json4s.jackson.JsonMethods = cons(0).newInstance().asInstanceOf[org.json4s.jackson.JsonMethods]
    jsonMethodObject
  }

  def parse(json:String):org.json4s.JsonAST.JValue = {
    try {
      if (parseMethod.getParameterCount == 2) {
        parseMethod.invoke(methodsObject, new StringInput(json), boolean2Boolean(false))
          .asInstanceOf[org.json4s.JsonAST.JValue]
      } else {
        parseMethod.invoke(methodsObject, new StringInput(json), boolean2Boolean(false), boolean2Boolean(true))
          .asInstanceOf[org.json4s.JsonAST.JValue]
      }
    }catch {
      case t:Throwable => {
        t.printStackTrace()
        throw t
      }
    }
  }
}
