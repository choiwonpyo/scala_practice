package com.sundogsoftware.spark.study001

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

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

    // Count up how many times each value (rating) occurs , 이때가 Action임.
    // 이때부터, 실행계획이 생성된다.
    // test file 불러오기, 여러 core 에서 데이터 작업 진행하게 된다.
    // 어? map 은 parallel 하게 진행할 수 있겠다? -> 그런데, 그 뒤엔 서로 얘기를 해야겠는걸. 결과를 순서대로 맞추자.(모집하자)
    // parallel mapping 이 한 stage 이고,
    // 그 다음에, countByValue 를 하려면 데이터 shuffle 이 일어난다. 따라서 이건 스테이지가 분리돼야 한다.
    // 모든 stage 는 task 로 분리된다.
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1) // sortBy 대상의, 2번째 (value).

    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}
