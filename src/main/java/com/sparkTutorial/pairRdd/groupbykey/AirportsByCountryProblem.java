package com.sparkTutorial.pairRdd.groupbykey;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AirportsByCountryProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,
           output the the list of the names of the airports located in each country.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           "Canada", ["Bagotville", "Montreal", "Coronation", ...]
           "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
           "Papua New Guinea",  ["Goroka", "Madang", ...]
           ...
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airportnotinusapairrdd").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.textFile("in/airports.text")
                .mapToPair(getPairFunction())
                .groupByKey()
                .foreach(tuple -> {
                    System.out.printf("%s : %s \n", tuple._1(), tuple._2());
                });
    }

    private static PairFunction<String, String, String> getPairFunction() {
        return line -> {
            String[] strArr = line.split(Utils.COMMA_DELIMITER);
            return new Tuple2<>(strArr[2], strArr[1]);
        };
    }
}
