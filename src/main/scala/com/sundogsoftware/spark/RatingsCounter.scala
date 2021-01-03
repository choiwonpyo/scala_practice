package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
 
  /** Our main function where the action happens */

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    // local[*] means, use all local cpu cores,
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("data/ml-100k/u.data")
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)<- 요 상태임,
    // 정확히는 userId 탭 movieId 탭 ... 요런 형
    val ratings = lines.map(x => x.split("\t")(2)) // 0, 1, 2 순서임.
    // 3
    // 3
    // 1
    // 2
    // 1

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()
    
    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1) // sortBy.
    
    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}
