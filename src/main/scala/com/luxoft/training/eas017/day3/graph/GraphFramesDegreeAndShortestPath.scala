package com.luxoft.training.eas017.day3.graph

import org.apache.spark.sql.SparkSession

//$ spark-shell --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 --conf spark.dynamicAllocation.maxExecutors=4 --conf spark.executor.memory=1g

object GraphFramesDegreeAndShortestPath extends App {

  val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Graph Frame Example")
      .getOrCreate()

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._
  import spark.implicits._

  val edgeDF = Seq(
    ("A", "B"),
    ("B", "C"),
    ("B", "D"),
    ("B", "E"),
    ("E", "F"),
    ("E", "G"),
    ("F", "G"),
    ("H", "I"),
    ("J", "I"),
    ("K", "L"),
    ("L", "M"),
    ("M", "N"),
    ("K", "N")
  ).toDF("src", "dst")


  import org.graphframes._
  val g = GraphFrame.fromEdges(edgeDF)


  // TODO: calculate vertices degrees from edgeDF using DataFrame API
  //  Let's assume that our graph is undirected, so you would need to sum in and out degrees

  val srcCount = edgeDF.groupBy("src")
    .agg(count("*").alias("cnt"))
    .withColumnRenamed("src", "id")

  val dstCount = edgeDF.groupBy("dst")
    .agg(count("*").alias("cnt"))
    .withColumnRenamed("dst", "id")

  // Union them together and sum the connecting count from both src and dst.
  val degrees = srcCount.union(dstCount)
    .groupBy("id")
    .agg(sum("cnt").alias("degree"))
    .sort("id")
  
  degrees.show()

  // TODO Compare your output with GraphFrames implementation
  g.degrees.sort("id").show()


  def mirrorEdges(edges: DataFrame): DataFrame = {
    val swapped = edges.selectExpr("dst as src", "src as dst")
    edges.union(swapped)
  }

  val appendToSeq = udf((x: Seq[String], y: String) => x ++ Seq(y))


  // It would be easier to implement shortestPath function first, order of functions was changed for copy-pasting to shell
  def shortestPathRecurse(paths: DataFrame, mirrored: DataFrame, end: String, iteration: Int = 0): DataFrame = {
    // TODO extend an existing path with next possible destination using 'appendToSeq' udf and returning it as a new 'dst'
    //  you would need to assign aliases to DataFrames to avoid column mismatch
    //  Eg: df.alias("a"), this alias can be used in 'col' function: col("a.colName")
    val sp : DataFrame = paths.alias("paths")
      .join(mirrored.alias("mirrored"), col("paths.dst") === col("mirrored.src"))
      .select(
        col("paths.src"),
        col("mirrored.dst"),
        appendToSeq(col("paths.path"), col("mirrored.dst")).alias("path")
      )

    sp.cache()

    // TODO: filter sp DataFrame, leaving only path's which lead to our destination "end" node
    val filtered : DataFrame = sp.where(s"dst = '$end'")

    if (filtered.count() > 0){
        filtered
    } else {
        shortestPathRecurse(sp, mirrored, end, iteration + 1)
    }
  }

  def shortestPath(edges: DataFrame, start: String, end: String): DataFrame = {
    // Mirror edges on the first iteration.
    val mirrored = mirrorEdges(edges)
    mirrored.cache()

    // Filter the edges to our starting vertex and init the path sequence.
    // TODO: Create a DataFrame of edges, outgoing from our "start" node
    //  with an additional column 'path', containing this edge as an array
    val paths : DataFrame = mirrored.where(s"src = '$start'")
      .select(
          mirrored("src"),
          mirrored("dst"),
          array(mirrored("src"), mirrored("dst")).alias("path")
      )

      // Recursively call until convergence
      val sp = shortestPathRecurse(paths, mirrored, end)
      sp.withColumn("path_length", size(sp("path")) - 1)
  }
  

  // Test your algorithm on some nodes
  shortestPath(edgeDF, "A", "G").show()

  shortestPath(edgeDF, "M", "K").show()

  //TODO: compare your output with GraphFrames implementation

  val spAG = g.shortestPaths.landmarks(Seq("A", "G")).run()

  spAG.orderBy("id").show()

  g.shortestPaths.landmarks(Seq("M", "K")).run().orderBy("id").show()

}
