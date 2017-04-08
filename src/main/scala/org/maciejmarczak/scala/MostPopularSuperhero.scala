package org.maciejmarczak.scala

import org.apache.spark.SparkContext

object MostPopularSuperhero {

  val SUPERHERO_GRAPH = "data/Marvel-graph.txt"
  val SUPERHERO_NAMES = "data/Marvel-names.txt"

  def parseGraph(line: String): (Int, Int) = {
    val splitted = line.split("\\s+")
    (splitted(0).toInt, splitted.length - 1)
  }

  def parseName(line: String): Option[(Int, String)] = {
    val splitted = line.split("\"")
    if (splitted.length > 1) {
      return Some((splitted(0).trim().toInt, splitted(1)))
    }
    None
  }

  def main(args: Array[String]): Unit = {

    val ctx = new SparkContext("local[*]", "MostPopularSuperhero")

    val names = ctx.textFile(SUPERHERO_NAMES).flatMap(parseName)
    val graph = ctx.textFile(SUPERHERO_GRAPH).map(parseGraph)

    val reducedGraph = graph.reduceByKey((x, y) => x + y)
    val flipped = reducedGraph.map(v => (v._2, v._1))
    val mostPopularSuperhero = flipped.max()

    val heroName = names.lookup(mostPopularSuperhero._2).head

    println(s"The most popular superhero is $heroName. He has ${mostPopularSuperhero._1} friends.")
  }

}
