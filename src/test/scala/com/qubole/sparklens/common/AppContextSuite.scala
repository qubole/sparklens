package com.qubole.sparklens.common

import com.qubole.sparklens.timespan.{ExecutorTimeSpan, StageTimeSpan}
import org.scalatest.FunSuite

class AppContextSuite extends FunSuite {

  test ("maximum concurrent executors at a time") {

    /*
     * We will generate this test case, and verify that we get "maximum" as the maximum
     * concurrent executors.


              ^
              |
              |
     maximum ---                ** *
              |                *   *
              |              **     *
              |             *       *
              |           **         *
              |          *           *
              |         *             *
        ^     |        *               *
        |     |        *                *
        |     |       *                 *
  numExecutors|      *                   *
              |     *                     *
              |    *                       *
              |    *                       *
              |   *                         *
              |  *                           *     *****         ****
              | *                             *   *     *       *    *
              |*                               *  *      *     *      *
              |                                * *       *    *        * ..
              +--------------------------------|---------------------------+
              0                            (2 * maximum)

                                          time ->
      */


    val random = scala.util.Random
    val size = 100000 // 100,000 executors

    // this will be answer for our test
    val maximum = random.nextInt(size / 2) + (size / 2)
    val maxExecutors = (1 to maximum).map(x => {

      val span = new ExecutorTimeSpan(x.toString, x.toString, x)
      span.startTime = x
      span.endTime = (2 * maximum) - x + 1
      (x.toString -> span)
    })

    // add remaining executors
    val remainingExecutors = ((maximum + 1) to size).map(x => {
      val span = new ExecutorTimeSpan(x.toString, x.toString, x)
      span.startTime = (2 * maximum) + x
      span.endTime = (2 * maximum) + x
      (x.toString -> span)
    })

    val startTime = System.currentTimeMillis()
    assert(AppContext.getMaxConcurrent(
      collection.mutable.HashMap((maxExecutors ++ remainingExecutors): _*)) == maximum)

    val endTime = System.currentTimeMillis()
    assert (endTime - startTime < 2000, s"Took ${endTime - startTime} ms to find " +
      s"maximum concurrent executors for ${size} executors")
  }
}
