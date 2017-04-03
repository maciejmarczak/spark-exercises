package org.maciejmarczak.scala

import org.apache.spark.SparkContext

object StationsTemperature {

  val DATA = "data/1800.csv"

  def parseLine(str: String) : (String, String, Double) = {
    val splitted = str.split(",")
    (splitted(0), splitted(2), splitted(3).toDouble * 0.1)
  }

  def main(args: Array[String]): Unit = {

    val ctx = new SparkContext("local[*]", "StationsTemperature")
    val lines = ctx.textFile(DATA)

    val parsed = lines.map(parseLine)
    val filteredOut = parsed.filter(v => v._2 == "TMIN").map(v => (v._1, v._3))

    val results = filteredOut.reduceByKey((x, y) => math.min(x, y)).collect()

    for (result <- results) {
      println(result)
    }
  }

}
