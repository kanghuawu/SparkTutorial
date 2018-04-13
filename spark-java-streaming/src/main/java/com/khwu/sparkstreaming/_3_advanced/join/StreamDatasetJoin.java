package com.khwu.sparkstreaming._3_advanced.join;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class StreamDatasetJoin {

    public static void main(String[] args) throws InterruptedException {
        Utilities.setUpLogging();

        SparkConf conf = new SparkConf()
                .setAppName("StreamDatasetJoin")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(4));

        String[] ips = new String[]{"data/ip_file1.txt", "data/ip_file2.txt"};
        List<String> lines = new LinkedList<>();
        for (int i = 0; i < ips.length; i++) {
            try (Scanner scanner = new Scanner(Paths.get(ips[i]))) {
                while (scanner.hasNextLine()) lines.add(scanner.nextLine());
                Thread.sleep(5_000);
            } catch (IOException e) {
                continue;
            }
        }
        JavaPairRDD<String, Integer> ipsDs = sc.parallelize(lines)
                .mapToPair(line -> new Tuple2<>(line, 1));

        JavaReceiverInputDStream<String> linesDs = ssc.socketTextStream("localhost", 9999);
        linesDs.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(ip -> new Tuple2<>(ip, 1))
                .transformToPair(pair -> pair.join(ipsDs))
                .print();

        ssc.start();
        Thread.sleep(30_000);
        ssc.stop(true, true);
    }
}
