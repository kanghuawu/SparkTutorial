package com.sparkTutorial._1_rdd.airports;

import com.sparkTutorial._1_rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */
        SparkConf conf = new SparkConf().setAppName("airportusa").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airportUSA = sc.textFile("in/airports.text")
                .filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""))
                .map(line -> {
                    String[] strArr = line.split(Utils.COMMA_DELIMITER);
                    return strArr[1] + ", " + strArr[2];
                });
        airportUSA.saveAsTextFile("out/airports_in_usa.text");
    }
}
