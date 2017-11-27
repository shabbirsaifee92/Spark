package com.shabbir.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.math.sqrt

object MoviesSimilarity {

  
  type ratingPair = (Int,Double)
  type userRatingPair = (Int, (ratingPair,ratingPair))
  def loadMovieNames() : Map[Int,String] = {
    
    //Handle character coding issues
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    var movieNames: Map[Int,String] = Map()
    
    //Read file
    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for(line <- lines){
      var fields = line.split('|')
      if(fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    
    
    return  movieNames
  }
  

  def parseLines(line: String) ={
    val fields = line.split("\t")
    (fields(0).toInt,(fields(1).toInt,fields(2).toDouble))
  }
  
  def removeDuplicates(pair: userRatingPair): Boolean = {
    
    val movieID1 = pair._2._1._1
    val movieID2 = pair._2._2._1
    
    return (movieID1 < movieID2)
  }
  
  def makePair(userPair:userRatingPair) = {
    
    val movie1 = userPair._2._1
    val movie2 = userPair._2._2
    
    ((movie1._1,movie2._1),(movie1._2,movie2._2))
  }
 
  type ratingPairs = Iterable[(Double,Double)]

  def computeCosineSimilarity(pairs:ratingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- pairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
  
  def main(args:Array[String]){
    //Set logger to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Spark Context
  val sc = new SparkContext("local[*]","MoviesSimilarity")
  
  println("Loading Movie Names... \n")
  
  //Load Movie names
  val nameDict = loadMovieNames()
  
  val data = sc.textFile("../ml-100k/u.data")
  
  //Map ratings to key-value pair=>  userId, (movieId, rating)
  val userRatings = data.map(parseLines)
  
  //Emit every movie rated by same user and get al the possible pairs of movies
  val joinedRatings = userRatings.join(userRatings)
  
  //At this point rdd consist of userId=> ((movieId,rating),(movieId,ratings))
  
  //Remove entries where both movie ids are same in a
  val uniqueJoinedRatings = joinedRatings.filter(removeDuplicates)
  
  //Key by (movie1,movie2)
  val moviePairs = uniqueJoinedRatings.map(makePair)
  
  //At this point we have rdd as ((movieID1,movieID2),(rating1,rating2))
  
  //group all the same pairs together
  val moviePairRating = moviePairs.groupByKey()
  
  //Now we have (movieID1,movieID2)=> [(rating1,rating2),(rating1,rating2)....]
  //Now compute the similarity between the two movies using the rating data
  
  val moviePairSimilarity = moviePairRating.mapValues(computeCosineSimilarity).cache()
  
  // Extract similarities for the movie we care about that are "good".
  
  if(args.length > 0){
    
    val threshold = 0.97
    val cooccurence = 50.0
    val movieID:Int = args(0).toInt
     
    val filteredResults = moviePairSimilarity.filter( x=>
      {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieID || pair._2 == movieID) && sim._1 > threshold && sim._2 > cooccurence    
      })
      
      //sort by quality score
      val results = filteredResults.map(x => (x._2,x._1)).sortByKey(false).take(10)
      
      println("\nTop 10 similar movies for " + nameDict(movieID))
      
          for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
      
    
    
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  }
  
}