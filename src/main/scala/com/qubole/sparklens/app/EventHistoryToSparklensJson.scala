package com.qubole.sparklens.app

import com.qubole.sparklens.helper.HDFSConfigHelper
import org.apache.hadoop.fs.{FileSystem, Path}

object EventHistoryToSparklensJson  {

  def main(args:Array[String]):Unit = {
    val defaultDestination = new Path("/tmp/sparklens/")

    val dirs = args.length match  {
      case 0 => (new Path("."), defaultDestination)
      case 1 => (new Path(args(0)), defaultDestination)
      case _ => (new Path(args(0)), new Path(args(1)))
    }
    println("Converting Event History files to Sparklens Json files")
    println(s"src: ${dirs._1.toUri.getRawPath} destination: ${dirs._2.toUri.getRawPath}")
    convert(dirs._1, dirs._2)
  }

  private def convert(srcLoc:Path, destLoc:Path): Unit = {
    val dfs = FileSystem.get(srcLoc.toUri, HDFSConfigHelper.getHadoopConf(None))

    if (dfs.getFileStatus(srcLoc).isFile) {
      try {
        new EventHistoryReporter(srcLoc.toUri.getRawPath, List(
          ("spark.sparklens.reporting.disabled", "true"),
          ("spark.sparklens.save.data", "true"),
          ("spark.sparklens.data.dir", destLoc.toUri.getRawPath)
        ))
      } catch {
        case e: Exception => {
          println(s"Failed to process file: ${srcLoc} error: ${e.getMessage}")
        }
      }
    } else {
      //This is a directory. Process all files
      dfs.listStatus(srcLoc).foreach( f => {
        convert(f.getPath, destLoc)
      })
    }
  }
}