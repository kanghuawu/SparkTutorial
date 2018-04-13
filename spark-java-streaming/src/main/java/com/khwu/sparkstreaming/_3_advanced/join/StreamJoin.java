package com.khwu.sparkstreaming._3_advanced.join;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamJoin {

    public static void main(String[] args) throws InterruptedException {
        Utilities.setUpLogging();

        SparkConf conf = new SparkConf()
                .setAppName("StreamJoin")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(4));

        Queue<JavaRDD<Integer>> queue = new LinkedList<>();
        for (int i = 0; i < 5; i++) {
            List<Integer> data = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
            JavaRDD<Integer> rdd = sc.parallelize(data);
            queue.add(rdd);
        }

        JavaPairDStream<Integer, Integer> ds1 = ssc.queueStream(queue)
                .mapToPair(x -> new Tuple2<>(x % 10, 1))
                .window(Durations.seconds(4))
                .reduceByKey((x, y) -> x + y);
        ds1.print();

        JavaPairDStream<Integer, Integer> ds2 = ssc.queueStream(queue)
                .mapToPair(x -> new Tuple2<>(x % 5, 1))
                .window(Durations.seconds(20))
                .reduceByKey((x, y) -> x + y);
        ds2.print();

        JavaPairDStream<Integer, Tuple2<Integer, Integer>> joinedStream = ds1.join(ds2);
        joinedStream.print();

        ssc.start();
        Thread.sleep(50_000);
        ssc.stop(true, true);
    }
}
