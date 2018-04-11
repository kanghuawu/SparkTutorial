package com.sparkTutorial._1_rdd.sumOfNumbers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */
        SparkConf conf = new SparkConf().setAppName("airportusa").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        int sum = sc.textFile("in/prime_nums.text")
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .filter(num -> !num.isEmpty())
                .map(num -> Integer.valueOf(num))
                .reduce((x, y) -> x + y);
        System.out.println(sum);
    }
}
