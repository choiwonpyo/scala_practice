package com.sundogsoftware.spark.study001

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalAmountSpentFkane {

  def parseLine(line:String): (String, Float) = {
    val fields = line.split(",")
    val userId = fields(0)
    val price = fields(2).toFloat
    (userId, price)
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)


    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalAmountSpentFkane")

    val lines = sc.textFile("data/customer-orders.csv")

    val userIdsAndPrices = lines.map(parseLine)

    val userPaids = userIdsAndPrices.reduceByKey((x, y) => x + y).map(target => (target._2, target._1)).sortByKey()
    //spark 가 자체지원하는 sortByKey 를 사용하면, cluster 모드로 사용할 때 이점이 있다. (성능 면)

    // 이 때 처음으로, 한 곳으로 데이터들이 모인다. (SHUFFLE 된 데이터들) _ 미리 collecti 해야 UserPaid 에서 제대로 된 값이 ㅣ나온다.
    val results = userPaids.collect()
    for(userPaid <- results) {
      println(s"userId: ${userPaid._2}, paid: ${userPaid._1}")
    }
  }
}
