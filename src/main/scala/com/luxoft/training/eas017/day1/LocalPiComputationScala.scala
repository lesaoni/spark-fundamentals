package com.luxoft.training.eas017.day1

import scala.math.random

object LocalPiComputationScala extends App {

  val numberOfIterations = 1000000;

  var pointsInsideCircle = 0;

  for (_ <- 0 to numberOfIterations){
    val x = random * 2 - 1
    val y = random * 2 - 1
    if (x * x + y * y < 1) pointsInsideCircle += 1

  }

  val piApproximation = 4.0 * pointsInsideCircle / numberOfIterations

  println("Pi is roughly " + piApproximation)

}
