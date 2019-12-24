package com.luxoft.training.eas017.day1;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class WordCountJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("RDD operations java")
                .getOrCreate();

        JavaRDD<String> text = spark.read().textFile( "src/main/resources/alice-in-wonderland.txt").javaRDD();

        //TODO
        //Lets count number of non empty lines
        long numberOfNonEmptyLines = 0;
        System.out.println("There are " + numberOfNonEmptyLines + " non empty lines");

        //TODO
        //Find what is the most frequent word length in text
        Integer mostFrequentWordLength = null;

        System.out.println("Most frequent word length in text is " + mostFrequentWordLength);

        //TODO
        //Print all distinct words for the most frequent word length
        List<String> words = null;

        System.out.println("Print all distinct words for the most frequent word length: " + words);
    }

}
