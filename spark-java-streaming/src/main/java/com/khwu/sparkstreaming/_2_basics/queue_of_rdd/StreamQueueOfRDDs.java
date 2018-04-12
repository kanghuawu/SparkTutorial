package com.khwu.sparkstreaming._2_basics.queue_of_rdd;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class StreamQueueOfRDDs {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("queueOfRdd");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        Utilities.setUpLogging();

        Queue<JavaRDD<Integer>> queue = new LinkedList<>();

        for (int i = 0; i < 5; i++) {
            List<Integer> li = new ArrayList<>();
            for (int j = 0; j < 1000; j++) {
                li.add(j + 1);
            }
            queue.add(ssc.sparkContext().parallelize(li, 10));
        }

        JavaDStream<Integer> inputStream = ssc.queueStream(queue);
        JavaPairDStream<Integer, Integer> mappedStream = inputStream
                .mapToPair(x -> new Tuple2<>(x % 10, 1));

        JavaPairDStream<Integer, Integer> reducedStream = mappedStream
                .reduceByKey((a, b) -> a + b);

        reducedStream.print();

        ssc.start();

        Thread.sleep(6000);
        ssc.stop(true, true);
    }
}
