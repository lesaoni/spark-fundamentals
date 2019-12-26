package com.luxoft.training.eas017.day2

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

/*
  $ spark-shell --packages com.databricks:spark-xml_2.11:0.7.0  --conf spark.dynamicAllocation.maxExecutors=4  --conf spark.executor.memory=1g
* */


object SQLScala {

  def main(args: Array[String]): Unit = {

    val hdfsPath = "hdfs:///user/centos/eas-017/sql/"

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SQL example scala")
      .getOrCreate()

    //load emails data to DataFrame
    val emails = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "item")
      .load(hdfsPath + "emails.xml")

    //print emails schema

    //register DataFrame as table

    //describe table

    //Write query to get all unique location values from emails

    //Write query to get all unique keyword values from top level `keyword` column
    //hint: use `explode` function to flatten array elements

    //Create table `shopping` based on `emails` table data with only `name`, `payment`, `quantity` and `shipping` columns

    //Select records from `shopping` table where `quantity` is greater then 1

    //Create table 'shipping_dates' that contains all `date` values from the `mail` top level column
    //hint: create intermediate dataFrames or tables to handle nesting levels


    //Register custom user defined function to parse date string with format MM/DD/YYYY into java.sql.Date type

    //Select unique and sorted records from `shipping_dates` table
    //hint: use `parseDate` udf to get correct sorting



    //Save `emails`, `shopping` and 'shipping_dates' table to json, csv and text files accordingly

  }
}
