package org.maciejmarczak.scala

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

object DegreesOfSeparation {

  val SUPERHERO_GRAPH = "data/Marvel-graph.txt"

  val startingCharacterId = 5306
  val targetCharacterId = 14

  val hitCounter: LongAccumulator = new LongAccumulator()

  type BFSData = (Array[Int], Int, String)
  type BFSNode = (Int, BFSData)

  def convertToBFSNode(line: String): BFSNode = {
    val fields = line.split("\\s+")
    val heroId = fields(0).toInt

    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for (connection <- 1 until fields.length) {
      connections += fields(connection).toInt
    }

    var color: String = "WHITE"
    var distance: Int = 9999

    if (heroId == startingCharacterId) {
      color = "GRAY"
      distance = 0
    }

    (heroId, (connections.toArray, distance, color))
  }

  def createStartingRdd(ctx: SparkContext): RDD[BFSNode] = {
    val lines = ctx.textFile(SUPERHERO_GRAPH)
    lines.map(convertToBFSNode)
  }

  def bfsMap(node: BFSNode): Array[BFSNode] = {
    val characterId: Int = node._1
    val data: BFSData = node._2

    val connections: Array[Int] = data._1
    val distance: Int = data._2
    var color: String = data._3

    var results: ArrayBuffer[BFSNode] = ArrayBuffer()

    if (color == "GRAY") {
      for (connection <- connections) {
        val newCharacterId = connection
        val newDistance = distance + 1
        val newColor = "GRAY"

        if (newCharacterId == targetCharacterId) {
          hitCounter.add(1)
        }

        val newEntry: BFSNode = (newCharacterId,
          (Array(), newDistance, newColor))

        results += newEntry
      }

      color = "BLACK"
    }

    val thisEntry: BFSNode = (characterId,
      (connections, distance, color))

    results += thisEntry
    results.toArray
  }

  def bfsReduce(data1: BFSData, data2: BFSData): BFSData = {
    val edges1: Array[Int] = data1._1
    val edges2: Array[Int] = data2._1
    val dist1: Int = data1._2
    val dist2: Int = data2._2
    val color1: String = data1._3
    val color2: String = data2._3

    var dist: Int = 9999
    var color: String = "WHITE"
    val edges: ArrayBuffer[Int] = ArrayBuffer()

    edges ++= (edges1 ++ edges2)
    dist = Math.min(dist, Math.min(dist1, dist2))

    if (color1 == "WHITE") {
      color = color2
    }
    if (color1 == "BLACK") {
      color = color1
    }
    if (color1 == "GRAY") {
      color = if (color2 == "BLACK") color2 else color1
    }

    (edges.toArray, dist, color)
  }

  def main(args: Array[String]): Unit = {

    val ctx = new SparkContext("local[*]", "DegreesOfSeparation")

    hitCounter.reset()
    var startingRdd = createStartingRdd(ctx)

    for (_ <- 1 to 10) {
      val mapped = startingRdd.flatMap(bfsMap)

      println("Processing " + mapped.count() + " values.")

      if (hitCounter.value > 0) {
        println(s"Hit the target! From ${hitCounter.value} directions.")
        return
      }

      startingRdd = mapped.reduceByKey(bfsReduce)
    }
  }
}
