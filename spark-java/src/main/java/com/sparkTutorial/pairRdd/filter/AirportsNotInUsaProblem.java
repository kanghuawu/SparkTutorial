package com.sparkTutorial.pairRdd.filter;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsNotInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airportnotinusapairrdd").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.textFile("in/airports.text")
                .map(line -> {
                    String[] strArr = line.split(Utils.COMMA_DELIMITER);
                    return new Tuple2(strArr[1], strArr[2]);
                })
                .filter(tuple -> !tuple._2.equals("\"United States\""))
                .saveAsTextFile("out/airports_not_in_usa_pair_rdd.text");
    }
}
