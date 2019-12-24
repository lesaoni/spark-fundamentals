package com.luxoft.training.eas017.day1

import org.apache.spark.SparkContext

object SparkHelloWorldScala extends App {

  val sc = new SparkContext("local[*]", "Hello World!")

  val localCollection = Seq("Hello", "World!")

  val distributedCollection = sc.parallelize(localCollection)

  distributedCollection.foreach(println)

  sc.stop()

}
