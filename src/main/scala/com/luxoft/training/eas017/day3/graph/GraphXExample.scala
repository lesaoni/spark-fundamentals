package com.luxoft.training.eas017.day3.graph

import org.apache.spark.SparkContext

object GraphXExample extends App {


  val sc = new SparkContext("local[*]", "Hello World!")

  import org.apache.spark.rdd.RDD
  import org.apache.spark.graphx._
  
  // type VertexId = Long
  // Edge [T] (srcId: VertexId, dstId: VertexId, attribute : T)

  val edges : RDD[Edge[Int]] = sc.parallelize(Seq(
    Edge(1L, 2L),
    Edge(1L, 3L),
    Edge(3L, 4L),
    Edge(5L, 6L)
  ))

  val graph = Graph.fromEdges(edges, 0)

  graph.numVertices

  // Get vertices
  graph.vertices.collect()

  // get edges
  graph.edges.collect()

  // cache
  graph.cache()

  // Change edge direction
  graph.reverse.edges.collect()

  //subgraph
  graph.subgraph(edgeTriplet => edgeTriplet.srcId == 1L || edgeTriplet.dstId == 6L).edges.collect()

  graph.subgraph(edgeTriplet => edgeTriplet.dstAttr == 0 ).edges.collect()

  // Degrees
  graph.degrees.collect()

  graph.inDegrees.collect()

  graph.outDegrees.collect()

  // map vertices

  // map edges

  // map triplets

  // Connected components - returns a new graph, with vertex property, representing lowest VertexId in the connected component
  graph.connectedComponents().vertices.collect()

  // Show lowest VertexId of each connected component
  graph.connectedComponents().vertices.map(vertexWithComponentRef => vertexWithComponentRef._2).distinct().collect()

  // Run PageRank algorithm
  graph.pageRank(tol = 0.01, resetProb = 0.15).vertices.sortBy(vp => vp._2, ascending = false)

  // type VertexId = Long
  val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin",     "student")), (7L, ("jgonzal", "postdoc")),
                         (5L, ("franklin", "prof")),    (2L, ("istoica", "prof"))))

  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                         Edge(2L, 5L, "colleague"), Edge(5L, 7L, "colleague")))


  val graphWithProperties = Graph(users, relationships)

  graphWithProperties.triplets.map(
    triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
  ).collect.foreach(println(_))

  
}
