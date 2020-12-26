package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, _}


object BostonCrimesMap extends App {

  val pathCrimes=args(0)
  val pathCodes=args(1)
  val pathOutput=args(2)

  val spark = SparkSession
    .builder()
    //.master( master="local[*]" ) // for local test, need to comment before jar assembly
    .getOrCreate()

  import spark.implicits._


  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(pathCrimes)


  val offenseCodes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(pathCodes)

  val offenseCodesBroadcast = broadcast(offenseCodes)

  val outAggregate = crimeFacts
    .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")

  val monthCount = outAggregate
    .groupBy($"DISTRICT", $"YEAR", $"MONTH")
    .count()

  monthCount.createOrReplaceTempView("monthCount")

  val medianaDf = spark.sql("select DISTRICT, sum(count) as crimes_total, approx_percentile(count, 0.5, 100) as crimes_monthly from monthCount group by DISTRICT")

  val avgDf = outAggregate
    .groupBy($"DISTRICT")
    .agg(avg($"Lat").alias("lat"), avg($"Long").alias("lng"))

  val typeDf= outAggregate
    .groupBy($"DISTRICT", $"NAME")
    .count()
    .withColumn("crime_type", split($"NAME", " - ")(0))
    .drop($"NAME")
    .groupBy($"DISTRICT", $"crime_type")
    .count()
    .orderBy(col("DISTRICT").desc, col("count").desc)
    .groupBy($"DISTRICT")
    .agg(collect_list("crime_type").alias("crime_list"))
    .withColumn("crime_list_3", slice(col("crime_list"),1,3))
    .withColumn("frequent_crime_types", concat_ws(", " , col("crime_list_3")))
    .orderBy(col("DISTRICT").desc)
    .drop("crime_list", "crime_list_3")


  val resultDf = avgDf
    .join(medianaDf, "DISTRICT")
    .join(typeDf, "DISTRICT")
    .orderBy("DISTRICT")



  resultDf
    .write
    .parquet(pathOutput)

  //resultDf.show(false)

}