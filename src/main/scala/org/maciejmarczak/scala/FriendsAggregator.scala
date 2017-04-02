package org.maciejmarczak.scala

import org.apache.spark.SparkContext

object FriendsAggregator {

  val DATA = "data/fakefriends.csv"

  def main(args: Array[String]): Unit = {

    val ctx = new SparkContext("local[*]", "FriendsAggregator")
    val lines = ctx.textFile(DATA)

    val friendsPerAge = lines
      // 0,Will,33,385 => (33, 385)
      .map(line => { val s = line.split(","); (s(2).toInt, s(3).toInt) })
      // (33, 385) => (33, (385, 1))
      .mapValues(v => (v, 1))

    // (33, (385, 1)) and (33, (2, 1)) => (33, (387, 2))
    val friendsSum = friendsPerAge.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // (33, (387, 2)) => (33, 193.5)
    val averageFriends = friendsSum
      .mapValues(v => v._1 / v._2)
      .collect()

    averageFriends.foreach(println)

  }

}
