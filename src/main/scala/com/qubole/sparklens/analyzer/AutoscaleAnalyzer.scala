package com.qubole.sparklens.analyzer
import com.qubole
import com.qubole.sparklens.autoscaling.{AutoscalingPolicy}
import com.qubole.sparklens.chart.{Graph, Point}
import com.qubole.sparklens.common.AppContext
import org.apache.spark.SparkConf

class AutoscaleAnalyzer(conf: SparkConf = new SparkConf) extends AppAnalyzer {
  val random = scala.util.Random

  override def analyze(ac: AppContext, startTime: Long, endTime: Long): String = {
    println("================== AutoScale Analyze ==================")
    val dimensions = qubole.sparklens.autoscaleGraphDimensions(conf)
    val coresPerExecutor = ac.executorMap.values.map(x => x.cores).sum / ac.executorMap.size
    println(s"cores per executor = ${coresPerExecutor}")
    val originalGraph = createGraphs(dimensions, ac, coresPerExecutor)

    ""
  }

  private def createGraphs(dimensions: List[Int], ac: AppContext, coresPerExecutor: Int): Graph = {

    val graph = new Graph(dimensions.head, dimensions.last)
    createActualExecutorGraph(ac, graph, 'o')
    createIdealPerJob(ac, graph, '*', coresPerExecutor)

    val realDuration = ac.appInfo.endTime - ac.appInfo.startTime
    println(s"\n\nTotal app duration = ${pd(realDuration)}")

    println(s"Maximum concurrent executors = ${graph.getMaxY()}")
    println(s"coresPerExecutor = ${coresPerExecutor}")
    println(s"\n\nIndex:\noooooo --> Actual number of executors")
    println("****** --> Ideal number of executors which would give same timelines")

    graph.plot('o', '*')
    graph
  }

  private def createActualExecutorGraph(appContext: AppContext, graph: Graph, graphIndex: Char)
  : Unit = {
    val sorted = AppContext.getSortedMap(appContext.executorMap, appContext)
    graph.addPoint(Point(appContext.appInfo.startTime, 0, graphIndex)) // start point
    var count: Int = 0
    sorted.map(x => {
      count += x._2.asInstanceOf[Int]
      graph.addPoint(Point(x._1, count, graphIndex))
    })
    graph.addPoint(Point(appContext.appInfo.endTime, 0, graphIndex))
  }

  private def createIdealPerJob(ac: AppContext, graph: Graph, graphIndex: Char,
                                coresPerExecutor: Int): Unit = {
    val maxConcurrentExecutors = AppContext.getMaxConcurrent(ac.executorMap, ac)

    graph.addPoint(Point(ac.appInfo.startTime, 0, graphIndex))
    var lastJobEndTime = ac.appInfo.startTime

    ac.jobMap.values.toSeq.sortWith(_.startTime < _.startTime).foreach(jobTimeSpan => {
      val optimalExecutors = jobTimeSpan.optimumNumExecutorsForJob(coresPerExecutor,
        maxConcurrentExecutors.asInstanceOf[Int])

      // first driver time when no jobs have run
      if (lastJobEndTime == ac.appInfo.startTime) graph.addPoint(Point(jobTimeSpan.startTime, 0,
        graphIndex))

      // If time gap between this job and last job is large
      if (jobTimeSpan.startTime - lastJobEndTime > AutoscalingPolicy.releaseTimeout) {
        graph.addPoint(Point(lastJobEndTime, 0, graphIndex))
      }

      graph.addPoint(Point(jobTimeSpan.startTime, optimalExecutors, graphIndex))
      graph.addPoint(Point(jobTimeSpan.endTime, optimalExecutors, graphIndex))
      lastJobEndTime = jobTimeSpan.endTime
    })
    graph.addPoint(Point(lastJobEndTime, 0, graphIndex))
    graph.addPoint(Point(ac.appInfo.endTime, 0, graphIndex))
  }
}
