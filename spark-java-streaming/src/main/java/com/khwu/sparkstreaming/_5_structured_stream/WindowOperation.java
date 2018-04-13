package com.khwu.sparkstreaming._5_structured_stream;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

public class WindowOperation {

    public static void main(String[] args) throws StreamingQueryException {

        Utilities.setUpLogging();

        SparkSession spark = SparkSession.builder()
                .appName("WindowOperation")
                .master("local[*]")
                .getOrCreate();

        StructType userSchema = new StructType()
                .add("userA", "string")
                .add("userB", "string")
                .add("timestamp", "timestamp")
                .add("interaction", "string");

        StructType twitterSchema = new StructType()
                .add("userA", "string");

        Dataset<Row> twitterID = spark.read()
                .schema(twitterSchema)
                .csv("data/twitterIDs.csv");

        Dataset<Row> csvDF = spark.readStream()
                .schema(userSchema)
                .csv("data/monitoring_data");

        Dataset<Row> joinedDF = csvDF.join(twitterID, "userA");

        Dataset<Row> interactions = joinedDF.select(joinedDF.col("userA"),
                joinedDF.col("interaction"),
                joinedDF.col("timestamp"));

        Dataset<Row> windowCount = interactions
                .groupBy(functions.window(interactions.col("timestamp"), "30 seconds", "10 seconds"))
                .count();

        StreamingQuery query = windowCount.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .option("numRow", "100")
                .trigger(Trigger.Once())
                .start();

        query.awaitTermination();
    }
}
