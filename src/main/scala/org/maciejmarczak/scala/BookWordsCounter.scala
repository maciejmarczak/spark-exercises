package org.maciejmarczak.scala

import org.apache.spark.SparkContext

object BookWordsCounter {

  val DATA = "data/book.txt"

  def main(args: Array[String]): Unit = {

    val ctx = new SparkContext("local[*]", "BookWordsCounter")
    val lines = ctx.textFile(DATA)

    val words = lines.flatMap(line => line.split("\\W+"))
    val normalizedWords = words.map(w => w.toLowerCase)

    // sorting on a cluster
    val countedWords = normalizedWords.map(w => (w, 1)).reduceByKey((x, y) => x + y)
    val sortedCountedWords = countedWords.map(v => (v._2, v._1)).sortByKey()

    for (word <- sortedCountedWords) {
      println(word)
    }

    // sorting in memory
    //val results = normalizedWords.countByValue()

    /*for (result <- results.toSeq.sortBy(_._2)) {
      println(result)
    }*/

  }

}
