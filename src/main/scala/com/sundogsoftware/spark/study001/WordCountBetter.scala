package com.sundogsoftware.spark.study001

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")

    // Load each line of my book into an RDD
    val input = sc.textFile("data/book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))// 이게 뭐지;;
    // \W 는 단어가 아닌 문자와 매칭 된다. ([^A-Za-z0-9_] 와 동일)
    // 반면 \w 는 단어와 매칭된다. 단어가 아닌 놈들의 연속이 오면, 그것들을 기준으로 SPLIT

    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase()) // to LowerCase

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.countByValue()

    // Print the results
    wordCounts.foreach(println)
  }

}
