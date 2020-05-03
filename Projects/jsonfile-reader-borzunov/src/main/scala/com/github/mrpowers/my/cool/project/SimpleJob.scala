package com.github.mrpowers.my.cool.project

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import spark.implicits._

object SimpleJob extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName(name = "Example")
    .master(master = "local[*]")
    .getOrCreate()
  val sc = spark.sparkContext
  val data = Array(1,2,3,4,5)
  val firstRDD: RDD[Int] = sc.parallelize(data)
  val result = firstRDD
    .map(x => x+1)
    .flatMap(x => List(x,x*2,x*3))
    .filter(x => x>10)
    .collect()
  println(result.toList)

  val rdd_get: Array[Row] = spark
    .read
    .format("json")
    .load("C:\\Users\\rusnib\\winmag\\winemag_single_row.json")
    .collect()

  val length = rdd_get.map(s => s.length)
  val total_length = length
    .reduce((a, b) => a+b)

  println(total_length)


  val datframe = spark.read.json("C:\\Users\\rusnib\\winmag\\winemag-data-130k-v2.json")
//  datframe.show()

  datframe.select("points").show()

  datframe.createOrReplaceTempView("wine")

  val tempSQL = spark.sql("SELECT count(*) as count_points FROM wine where points > 87").show()
}
