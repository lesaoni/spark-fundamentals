package com.luxoft.training.eas017.day1;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
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
        long numberOfNonEmptyLines = text.filter(line -> !line.isEmpty()).count();
        System.out.println("There are " + numberOfNonEmptyLines + " non empty lines");

        //TODO
        //Find what is the most frequent word length in text
        Integer mostFrequentWordLength = text.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .map(word -> word.toLowerCase().replaceAll("[^a-z]", ""))
                .keyBy(word -> word.length())
                .aggregateByKey(0, (count, word) -> count + 1, (count1, count2) -> count1 + count2)
                .mapToPair(tuple -> tuple.swap())
                .sortByKey(false)
                .values()
                .first();

        System.out.println("Most frequent word length in text is " + mostFrequentWordLength);

        //TODO
        //Print all distinct words for the most frequent word length
        List<String> words = text.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .map(word -> word.toLowerCase().replaceAll("[^a-z]", ""))
                .filter(word -> word.length() == mostFrequentWordLength)
                .collect();
        System.out.println("Print all distinct words for the most frequent word length: " + words);
    }

}
