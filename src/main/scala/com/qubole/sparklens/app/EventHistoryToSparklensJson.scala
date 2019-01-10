package com.qubole.sparklens.app

import java.io.File

object EventHistoryToSparklensJson  {

  def main(args:Array[String]):Unit = {
    val defaultDestination = new File("/tmp/sparklens/")

    val dirs = args.length match  {
      case 0 => (new File("."), defaultDestination)
      case 1 => (new File(args(0)), defaultDestination)
      case _ => (new File(args(0)), new File(args(1)))
    }
    println("Converting Event History files to Sparklens Json files")
    println(s"src: ${dirs._1.getAbsolutePath} destination: ${dirs._2.getAbsolutePath}")
    convert(dirs._1, dirs._2)
  }

  private def convert(srcLoc:File, destLoc:File): Unit = {
    if (srcLoc.isFile) {
      try {
        new EventHistoryReporter(srcLoc.getAbsolutePath, List(
          ("spark.sparklens.reporting.disabled", "true"),
          ("spark.sparklens.save.data", "true"),
          ("spark.sparklens.data.dir", destLoc.getAbsolutePath)
        ))
      } catch {
        case e: Exception => {
          println(s"Failed to process file: ${srcLoc} error: ${e.getMessage}")
        }
      }
    } else {
      //This is a directory. Process all files
      srcLoc.listFiles().foreach( f => {
        convert(f, destLoc)
      })
    }
  }
}