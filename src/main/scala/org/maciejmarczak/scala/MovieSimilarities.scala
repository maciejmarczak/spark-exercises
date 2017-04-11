package org.maciejmarczak.scala

import java.nio.charset.CodingErrorAction

import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object MovieSimilarities {

  def loadMovieNames() : Map[Int, String] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("data/u.item").getLines()

    for (line <- lines) {
      val fields = line.split("\\|")
      movieNames += (fields(0).toInt -> fields(1))
    }

    movieNames
  }

  type Rating = (Int, Double)

  def filterDuplicates(ratingPair: (Int, (Rating, Rating))): Boolean = {
    val firstRating = ratingPair._2._1
    val secondRating = ratingPair._2._2

    firstRating._1 < secondRating._2
  }

  def parseLine(line: String): (Int, Rating) = {
    val fields = line.split("\t")
    (fields(0).toInt, (fields(1).toInt, fields(2).toDouble))
  }

  def makePairs(ratings: (Int, (Rating, Rating))): ((Int, Int), (Double, Double)) = {
    val firstRating = ratings._2._1
    val secondRating = ratings._2._2

    (firstRating._1, secondRating._1) -> (firstRating._2, secondRating._2)
  }

  def computeCosineSimilarity(ratingPairs: Iterable[(Double, Double)]): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = Math.sqrt(sum_xx) * Math.sqrt(sum_yy)

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    (score, numPairs)
  }

  def main(args: Array[String]): Unit = {

    val ctx = new SparkContext("local[*]", "MovieSimilarities")
    val movieNames = loadMovieNames()

    val data = ctx.textFile("data/u.data")
    val ratings = data.map(parseLine)

    val joinedRatings = ratings.join(ratings)
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    val moviePairsRatings = moviePairs.groupByKey()
    val moviePairSimilarities = moviePairsRatings
      .mapValues(computeCosineSimilarity).cache()

    if (args.length > 0) {
      val scoreThreshold = 0.60
      val coOccurenceThreshold = 50.0

      val movieId: Int = args(0).toInt
      val filteredResults = moviePairSimilarities.filter(x => {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieId || pair._2 == movieId) &&
          sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
      })

      val results = filteredResults.map(x => (x._2, x._1))
        .sortByKey(ascending = false).take(10)

      println("\nTop similar movies (max of 10) for " + movieNames(movieId))

      for (result <- results) {
        val sim = result._1
        val pair = result._2

        var similarMovieId = pair._1
        if (similarMovieId == movieId) {
          similarMovieId = pair._2
        }
        println(movieNames(similarMovieId) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
  }

}
