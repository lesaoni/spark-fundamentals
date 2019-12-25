package com.luxoft.training.eas017.day1;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkPiComputationJava {

    public static void main(String[] args) {

       SparkContext sc = new SparkContext("local[*]", "Hello World!");
       JavaSparkContext javaSparkContext = new JavaSparkContext(sc);

       int numberOfIterations = 1000000;
       
       // TODO: Calculate Pi using Spark

       double piApproximation = 3.14;

       System.out.println("Pi is roughly " + piApproximation);

    }
}
