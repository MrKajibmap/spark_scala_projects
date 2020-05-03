package com.github.mrpowers.my.cool.project

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object crime_analys extends App  {

    val spark = SparkSession
      .builder()
      .master(master = "local[*]")
      .appName("Crime")
      .getOrCreate()


    import spark.implicits._

    val sc = spark.sparkContext

    val df1: DataFrame = spark.read.json("C:\\libs\\clickstream_data\\omni_clickstream.json")
    val df2: DataFrame = spark.read.json("C:\\libs\\clickstream_data\\products.json")
    val df3: DataFrame = spark.read.json("C:\\libs\\clickstream_data\\users.json")

    val get_1: Unit = df1.createOrReplaceTempView("omni_clickstream")
    val get_2: Unit = df2.createOrReplaceTempView("products")
    val get_3: Unit = df3.createOrReplaceTempView("users")

//    df1.show()
//    df2.show()
//    df3.show()

    df1
      .join(df2, df1("url") === df2("url"))
    .join(df3,df1("swid") === concat(lit("{"),df3("SWID"), lit("}")))
      .filter($"GENDER_CD" =!= "U")
      .groupBy("GENDER_CD", "category")
      .count()
      .orderBy($"GENDER_CD", $"count")
    .show()

    case class MainClass(id: Int , country: String , points: Int , title: String , variety : String , winery: String)

    val tsvWithHeaderOptions: Map[String, String] = Map(
        ("delimiter", ","), // Uses "\t" delimiter instead of default ","
        ("header", "true"))
    println(tsvWithHeaderOptions)

    df2.coalesce(1)         // Writes to a single file
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("C:\\Users\\rusnib\\temp\\output")
}
