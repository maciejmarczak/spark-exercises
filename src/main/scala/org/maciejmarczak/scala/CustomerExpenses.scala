package org.maciejmarczak.scala

import org.apache.spark.SparkContext

object CustomerExpenses {

  val DATA = "data/customer-orders.csv"

  def parseLine(line: String): (Int, Double) = {
    val splitted = line.split(",")
    (splitted(0).toInt, splitted(2).toDouble)
  }

  def main(args: Array[String]): Unit = {

    val ctx = new SparkContext("local[*]", "CustomerExpenses")
    val lines = ctx.textFile(DATA)

    val orders = lines.map(parseLine)
    val totalExpenses = orders.reduceByKey((x, y) => x + y)

    val results = totalExpenses.collect()

    for (result <- results.sortBy(_._2)) {
      println(result)
    }

  }

}
