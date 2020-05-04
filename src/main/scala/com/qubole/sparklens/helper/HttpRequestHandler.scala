package com.qubole.sparklens.helper

import org.apache.http.client.methods.RequestBuilder
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.HttpMultipartMode
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.HttpClients
import java.io.File

import org.apache.http.HttpResponse

object HttpRequestHandler {

  def requestReport(fileName: String, email: String): HttpResponse = {
    val httpclient = HttpClients.createDefault()
    val file = new File(fileName)
    val builder = MultipartEntityBuilder.create()
      .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
      .addBinaryBody("file-2", file, ContentType.DEFAULT_BINARY, file.getName())
      .addTextBody("email", email)
    val postData = builder.build()
    val request = RequestBuilder
      .post("http://sparklens.qubole.com/generate_report/request_generate_report")
      .setEntity(postData)
      .build()
    val response = httpclient.execute(request)
    response
  }
}
