package com.khwu.sparkstreaming._2_basics.sql;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

public class SqlOperations {

    public static void main(String[] args) throws InterruptedException {

        Utilities.setUptTwitter();

        JavaStreamingContext ssc = new JavaStreamingContext("local[*]",
                "SqlOperations",
                Durations.seconds(2));

        Utilities.setUpLogging();

        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(ssc);

        tweets.foreachRDD(rdd -> {
            SparkSession spark = SparkSession.builder().config(rdd.rdd().sparkContext().getConf()).getOrCreate();


            JavaRDD<JavaRow> rowRDD = rdd.map(status -> {
                JavaRow record = new JavaRow();
                record.setUser(status.getUser().getName());
                record.setText(status.getText());
                return record;
            });

            Dataset<JavaRow> dataset = spark.createDataset(rowRDD.rdd(), Encoders.bean(JavaRow.class));
            dataset.show();
        });

        ssc.start();
        Thread.sleep(10_000);
        ssc.stop(true, true);
    }
}

