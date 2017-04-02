package org.maciejmarczak.scala

import org.apache.spark.SparkContext

object RatingsCounter {

  val DATA = "data/u.data"

  def main(args: Array[String]): Unit = {

    val ctx = new SparkContext("local[*]", "RatingsCounter")
    val lines = ctx.textFile(DATA)

    val ratings = lines.map(line => line.split("\t")(2))
    val results = ratings.countByValue()
    val sortedResults = results.toSeq.sortBy(_._1)

    sortedResults.foreach(println)

  }

}
