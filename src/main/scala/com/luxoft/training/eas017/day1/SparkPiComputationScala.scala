package com.luxoft.training.eas017.day1

import org.apache.spark.SparkContext

import scala.math.random

object SparkPiComputationScala extends App {

  val numberOfIterations = 1000000

  val sc = new SparkContext("local[*]", "Pi computation")

  // TODO: Calculate Pi using Spark

  val piApproximation = ???

  println("Pi is roughly " + piApproximation)

}
