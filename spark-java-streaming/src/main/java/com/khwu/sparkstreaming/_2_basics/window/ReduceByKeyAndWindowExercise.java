package com.khwu.sparkstreaming._2_basics.window;

import com.khwu.sparkstreaming.util.ApacheAccessLog;
import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Objects;

public class ReduceByKeyAndWindowExercise {

    public static void main(String[] args) throws InterruptedException {
        Utilities.setUptTwitter();

        JavaStreamingContext ssc = new JavaStreamingContext("local[*]",
                "ReduceByKeyAndWindowExercise",
                Durations.seconds(1));

        ssc.checkpoint("checkpoint");
        Utilities.setUpLogging();

        JavaDStream<String> lines = ssc.textFileStream("file:///Users/khwu/Projects/SparkTutorial/spark-java-streaming/logs");

        lines.map(ApacheAccessLog::parseFromLogLine)
                .filter(Objects::nonNull)
                .mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1))
                .reduceByKey((x, y) -> x + y)
                .print();

        ssc.start();
        Thread.sleep(30000);
        ssc.stop(true, true);
    }
}
