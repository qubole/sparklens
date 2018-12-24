package com.qubole.sparklens.chart

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Graph(width: Int = 100, length: Int = 30) {

  private val compressedPointsSet = mutable.HashMap[Char, mutable.ListBuffer[Point]]()
  private val pointsSet = mutable.HashMap[Char, mutable.ListBuffer[Point]]()

  var graphString = mutable.ListBuffer[StringBuilder]()

  private def init = {
    // 10 % buffer
    (1 to length  + (length / 10 ) ).foreach(_ => {
      val builder = new StringBuilder()
      (1 to width + (width / 10)).foreach(_ => builder.append(" "))
      graphString.append(builder)
    })
  }

  init

  def addPoint(p: Point) = {
    val list = if (pointsSet.get(p.c).isDefined) {
      pointsSet(p.c)
    } else new mutable.ListBuffer[Point]()
    list.append(p)
    pointsSet.put(p.c, list)
  }


  def plot(original: Char = 'o', simulated: Char = '*'): Unit = {
    // find max of all graphs
    val maxY = getMaxY()

    pointsSet.keys.foreach(c => {
      fixYLimit(c, maxY)
      plot(c)
    })
    var topFlag = true

    //markOptimizationSpace(original, simulated)

    graphString.reverse.foreach(x => {
      topFlag match {
        case false => print("  |  ")
        case true =>
          print("  ^  ")
          topFlag = false
      }
      println(x)
    })

    (1 to (width / 2)).foreach(_ => print("-"))
    print(" time ")
    (1 to (width / 2)).foreach(_ => print("-"))
    println(">")

  }

  def getMaxY(): Int = {
    pointsSet.values.map(_.maxBy(_.y)).maxBy(_.y).y
  }

  def getMaxForChar(c: Char): Int = {
    pointsSet(c).map(_.y).max
  }

  private def plot(c: Char): Unit = {
    val widestGraph = compressedPointsSet.values.toSeq.maxBy(x => x.last.x - x.head.x)

    val xJump = (widestGraph.last.x - widestGraph.head.x) / width
    xJump match {
      case 0 => // don't bother of spark-apps lasting less than *length* millisecond
      case _ => largerGraph(compressedPointsSet(c), xJump)
    }
  }


  private def fixYLimit(c: Char, maxY: Int) {

    val points = pointsSet(c)

    maxY match {
      case large if large >= length => {

        val newPoints = mutable.ListBuffer[Point]()

        points.foreach(p => {
          val newY: Int = (length * p.y) / maxY
          //println(s"changing ${p.y} to ${newY}")
          newPoints.append(Point(p.x, newY, p.c))
        })
        compressedPointsSet.put(c, newPoints)
      }
      case _ => compressedPointsSet.put(c, points)
    }
  }


  private def largerGraph(sortedPoints: ListBuffer[Point], xJump: Long): Unit = {
    val qu = scala.collection.mutable.Queue.empty[Point]
    sortedPoints.foreach(qu.enqueue(_))
    //val qu = scala.collection.immutable.Queue(sortedPoints: _*)

    var index = 0
    var lastPoint: Point = null
    var start = sortedPoints.head.x
    var nextTarget = start + (index * xJump)

    while (!qu.isEmpty) {
      val front = qu.dequeue

      // if nextTarget is still far, update the current index
      if (nextTarget > front.x || lastPoint == null) {
        //fill the graph
        if (lastPoint == null) {
          graphString(front.y).setCharAt(index, front.c)
        } else {
          (lastPoint.y + 1 to front.y).foreach(y => graphString(y).setCharAt(index, front.c))
          (front.y to lastPoint.y - 1).foreach(y => graphString(y).setCharAt(index, front.c))
        }

      } else {
        val ep = extrapolationPoints(front.x, nextTarget, xJump)
        val yJumps = (front.y - lastPoint.y) / ep
        var lastY = lastPoint.y
        (1 to ep).foreach(i => {
          val y = if (i == ep) front.y
          else lastPoint.y + (i * yJumps)
          // fill the graph
          (lastY + 1 to y).foreach(interimY => graphString(interimY).setCharAt(index, front.c))
          (y to lastY - 1).foreach(interimY => graphString(interimY).setCharAt(index, front.c))
          graphString(y).setCharAt(index, front.c)
          lastY = y
          index += 1
        })
      }
      lastPoint = front
      nextTarget = start + (index * xJump)
    }
  }

  private def extrapolationPoints(xPoint: Long, target: Long, xJump: Long): Int = {
    var tmp = target
    var jumps = 1
    while (tmp + xJump <= xPoint) {
      tmp += xJump
      jumps += 1
    }
    jumps
  }

}
