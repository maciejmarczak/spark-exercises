package org.maciejmarczak.scala

import java.nio.charset.CodingErrorAction

import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object MostPopularMovie {

  val DATA = "data/u.data"
  val MOVIE_TITLES = "data/u.item"

  def parseLine(line: String): (Int, Int) = {
    (line.split("\t")(1).toInt, 1)
  }

  def loadMovieTitles(): Map[Int, String] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile(MOVIE_TITLES).getLines()
    for (line <- lines) {
      val splitted = line.split("\\|")
      if (splitted.length > 1) {
        movieNames += (splitted(0).toInt -> splitted(1))
      }
    }

    movieNames
  }

  def main(args: Array[String]): Unit = {

    val ctx = new SparkContext("local[*]", "MostPopularMovie")
    val namedDict = ctx.broadcast(loadMovieTitles())

    val lines = ctx.textFile(DATA)

    val movies = lines.map(parseLine)
    val moviesCount = movies.reduceByKey((x, y) => x + y)
    val sortedMoviesCount = moviesCount.sortBy(_._2)

    val results = sortedMoviesCount
      .map(m => (namedDict.value(m._1), m._2))
      .collect()

    for (movieCount <- results) {
      println(movieCount)
    }
  }

}
