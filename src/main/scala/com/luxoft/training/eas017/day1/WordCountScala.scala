package com.luxoft.training.eas017.day1

import org.apache.spark.sql.SparkSession


object WordCountScala {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Word count")
      .getOrCreate()

    val text = spark.sparkContext.textFile("src/main/resources/alice-in-wonderland.txt")

    //TODO
    //Lets count number of non empty lines
    val numberOfNonEmptyLines = ???
    println(s"There are $numberOfNonEmptyLines non empty lines")

    //TODO
    //Find what is the most frequent word length in text
    val mostFrequentWordLength: Int =  ???

    println(s"Most frequent word length in text is $mostFrequentWordLength")

    //TODO
    //Print all distinct words for the most frequent word length
    val words: Seq[String] = ???

    println(s"Print all distinct words for the most frequent word length: ${words.mkString(", ")}")
  }
}
