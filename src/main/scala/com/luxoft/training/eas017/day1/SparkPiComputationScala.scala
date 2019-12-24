package com.luxoft.training.eas017.day1

import org.apache.spark.SparkContext

import scala.math.random

object SparkPiComputationScala extends App {

  val numberOfIterations = 1000000

  val sc = new SparkContext("local[*]", "Pi computation")

  // TODO: Calculate Pi using Spark
  val iterationsRdd = sc.parallelize(1 to numberOfIterations)

  val iterationsOutcomes = iterationsRdd.filter(_ => {
    val x = random * 2 - 1
    val y = random * 2 - 1
    x * x + y * y < 1
  })

  val pointsInsideCircle = iterationsOutcomes.count

  val piApproximation = 4.0 * pointsInsideCircle / numberOfIterations

  println("Pi is roughly " + piApproximation)

}
