package com.luxoft.training.eas017.day1

import org.apache.spark.SparkContext

/*
 $ spark-submit  \
 --master yarn  \
 --class com.luxoft.training.eas017.day1.HelloWorldYarn  \
 hdfs:///user/centos/eas-017/eas-017-spark-hello-world.jar \
 *arg0 - your result filename*
*/


object HelloWorldYarn extends App {

  val hdfsPath = "hdfs:///user/centos/eas-017/hello/"

  val fileName = args(0)

  val sc = new SparkContext()

  val localCollection = Seq("Hello", "World!")

  val distributedCollection = sc.parallelize(localCollection, 2)

  distributedCollection.saveAsTextFile(hdfsPath + fileName)

}
