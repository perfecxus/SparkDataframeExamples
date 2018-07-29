package com.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sinchan on 29/07/18.
  */
object Driver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("DataframeApp").getOrCreate()

    val df = spark.read.format("csv").option("header","true").load("/Users/sinchan/Documents/Coursera/BigData & MachineLearning/big-data-4/daily_weather.csv")

    df.printSchema()

    df.show()

    //df.describe()

    val df2 = df.select("air_pressure_9am","air_temp_9am")

    //spark.sql("select air_pressure_9am,air_temp_9am from df2view")

    df2.show()

    df2.printSchema()

    println(s"df2 count ${df2.count()}")

    df2.describe("air_pressure_9am").show()

    df2.describe("air_temp_9am").show()

    val df3 = df2.na.fill(64.93300141287072,Seq("air_temp_9am")).na.fill(918,Seq("air_pressure_9am"))

    //df2.na.

    println(s"df3 count ${df3.count()}")

    df3.printSchema()

    df3.createOrReplaceTempView("df3view")

    val df4 = spark.sql("select round(CAST(air_pressure_9am as double)) as air_pressure_9am,CAST(air_temp_9am as double) as air_temp_9am from df3view")

    df4.printSchema()

    df4.show()

    df4.createOrReplaceTempView("df4")

    println(df4.stat.corr("air_pressure_9am","air_temp_9am"))


    df.describe().show()

    //import spark.implicits._
    //val df5 = df4.groupBy("air_pressure_9am").avg("air_temp_9am").select($"air_pressure_9am",$"avg(air_temp_9am)".alias("avg_temp_9am"))

    //spark.sql("select air_pressure_9am,avg(air_temp_9am) as avg_temp_9am from df4 group by air_pressure_9am")

    //df5.show()


  }

}
