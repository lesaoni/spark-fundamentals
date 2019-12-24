package com.luxoft.training.eas017.day1;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkHelloWorldJava {

   public static void main(String[] args) {

       SparkContext sc = new SparkContext("local[*]", "Hello World!");

       JavaSparkContext javaSc = new JavaSparkContext(sc);

       List<String> localCollection = Arrays.asList("Hello", "World!");

       JavaRDD<String> distributedCollection = javaSc.parallelize(localCollection);

       distributedCollection.foreach(s -> System.out.println(s));

       javaSc.stop();

   }

}
