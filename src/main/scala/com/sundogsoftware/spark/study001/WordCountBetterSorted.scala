package com.sundogsoftware.spark.study001

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSorted {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

     // Create a SparkContext using the local machine (local[*] 이 아니라 local 인걸 주의)
    val sc = new SparkContext("local", "WordCountBetterSorted")

    // Load each line of my book into an RDD
    val input = sc.textFile("data/book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y ) // countByValue 를 직접 구현. (SHUFFLED RDD)
    // reduceByKey 까지는 어느정도 locally 하게 진행함.

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey() // 여기까지도 RDD (SHUFFLED RDD)

    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) { // 이 때 처음 RDD 에서 풀림. 아마 이때가 ACTION 으로 치는 것인듯?
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }

}
