package com.luxoft.training.eas017.day3.graph

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

//$ spark-shell --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 --conf spark.dynamicAllocation.maxExecutors=4  --conf spark.executor.memory=1g

object GraphFramesExample extends App {

  val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Graph Frame Example")
      .getOrCreate()

  val vertices = spark.createDataFrame(List(
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
    ("d", "David", 29),
    ("e", "Esther", 32),
    ("f", "Fanny", 36),
    ("g", "Gabby", 60)
  )).toDF("id", "name", "age")

  val edges = spark.createDataFrame(List(
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
    ("f", "c", "follow"),
    ("e", "f", "follow"),
    ("e", "d", "friend"),
    ("d", "a", "friend"),
    ("a", "e", "friend")
  )).toDF("src", "dst", "relationship")

  val friends = GraphFrame(vertices, edges) // this graph available as org.graphframe.examples.Graphs.friends

  friends.toGraphX

}
