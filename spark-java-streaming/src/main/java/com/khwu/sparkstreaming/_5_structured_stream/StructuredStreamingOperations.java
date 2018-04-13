package com.khwu.sparkstreaming._5_structured_stream;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class StructuredStreamingOperations {

    public static void main(String[] args) throws StreamingQueryException {
        Utilities.setUpLogging();

        SparkSession spark = SparkSession.builder()
                .appName("StructcuredStreaming")
                .master("local[*]")
                .getOrCreate();

        StructType schema = new StructType()
                .add("time", "string")
                .add("oId", "integer")
                .add("cId", "integer")
                .add("qty", "integer")
                .add("price", "float")
                .add("buy", "string");

        Dataset<Row> lines = spark.readStream()
                .schema(schema)
                .csv("data/monitoring_data");

        Dataset<Row> buySellCount = lines.groupBy("buy").count();

        StreamingQuery query = buySellCount.writeStream().outputMode("complete").format("console").start();
        query.awaitTermination();
    }
}
