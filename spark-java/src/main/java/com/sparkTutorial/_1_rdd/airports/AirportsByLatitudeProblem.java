package com.sparkTutorial._1_rdd.airports;

import com.sparkTutorial._1_rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsByLatitudeProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */
        SparkConf conf = new SparkConf().setAppName("airportusa").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.textFile("in/airports.text")
                .filter(line -> Double.valueOf(line.split(Utils.COMMA_DELIMITER)[6]) > 40)
                .map(line -> {
                    String[] strArr = line.split(Utils.COMMA_DELIMITER);
                    return StringUtils.join(new String[]{strArr[1], strArr[6]}, ",");
                })
                .saveAsTextFile("out/airports_by_latitude.text");
    }
}
