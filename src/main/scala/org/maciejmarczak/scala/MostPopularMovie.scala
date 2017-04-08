package org.maciejmarczak.scala

import org.apache.spark.SparkContext

object MostPopularMovie {

  val DATA = "data/u.data"

  def parseLine(line: String): (Int, Int) = {
    (line.split("\t")(1).toInt, 1)
  }

  def main(args: Array[String]): Unit = {

    val ctx = new SparkContext("local[*]", "MostPopularMovie")
    val lines = ctx.textFile(DATA)

    val movies = lines.map(parseLine)
    val moviesCount = movies.reduceByKey((x, y) => x + y)
    val sortedMoviesCount = moviesCount.sortBy(_._2)

    val results = sortedMoviesCount.collect()

    for (movieCount <- results) {
      println(movieCount)
    }
  }

}
