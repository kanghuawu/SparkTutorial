package com.sparkTutorial._2_pairRdd.sort;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airportnotinusapairrdd").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.textFile("in/word_count.text")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))
                .sortByKey(false)
                .mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))
                .foreach(tuple -> System.out.printf("%s : %d\n", tuple._1(), tuple._2()));

    }
}

