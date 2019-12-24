package com.luxoft.training.eas017.day1;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.stream.IntStream;

import static java.lang.Math.random;
import static java.util.stream.Collectors.toList;

public class SparkPiComputationJava {

    public static void main(String[] args) {

       SparkContext sc = new SparkContext("local[*]", "Hello World!");
       JavaSparkContext javaSparkContext = new JavaSparkContext(sc);

       int numberOfIterations = 1000000;

       List<Integer> range = IntStream.range(1, 100000).boxed().collect(toList());

       // TODO: Calculate Pi using Spark
       int pointsInsideCircle = javaSparkContext
            .parallelize(range)
            .map(i -> {
                double x = random() * 2 - 1;
                double y = random() * 2 - 1;
                return x * x + y * y < 1 ? 1 : 0;
            }).reduce((sum1, sum2) -> sum1 + sum2);

       double piApproximation = 4.0 * pointsInsideCircle / numberOfIterations;

       System.out.println("Pi is roughly " + piApproximation);

    }
}
