package com.refactorlabs.cs378.assign12;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

public class DataSets {
    public static void main(String[] args) {
        
        String inputFilename = args[0];
        String outputFilename = args[1];
        SparkConf sp_conf = new SparkConf().setAppName(DataSets.class.getName()).setMaster("local");
        SparkContext spc = new SparkContext();
        SparkSession sp_session = SparkSession.builder().master("local").appName("UserSessionData").config("spark.sql.warehouse.dir", "file:///C:/Users/Mridhul/Desktop/Spark/spark-2.0.0-bin-hadoop2.7").getOrCreate();
        Dataset<Row> info = sp_session.read().option("header", true).csv(inputFilename);
        
        info.registerTempTable("table1");
        Dataset<Row> temp_table = sp_session.sql("SELECT DISTINCT vin, year, make, model, price, mileage, event FROM table1 ");
        try {
            temp_table.createTempView("distinctTable");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
                
        Dataset<Row> price_data = sp_session.sql("SELECT make, model, MIN(CAST(price AS INT)) as min_price, MAX(CAST(price AS INT)) as max_price, AVG(price) as avg_price FROM distinctTable WHERE price != 0 GROUP BY make, model ORDER BY make, model");
        Dataset<Row> mileage_data = sp_session.sql("SELECT year, MIN(CAST(mileage AS INT)) as min_mileage, MAX(CAST(mileage AS INT)) as max_mileage, AVG(mileage) as avg_mileage FROM distinctTable WHERE mileage > 0 GROUP BY year ORDER BY year");
        Dataset<Row> event_data = sp_session.sql("SELECT vin, event, COUNT(event) as count FROM (SELECT vin, SUBSTRING_INDEX(event, ' ', 1) as event from table1) GROUP BY vin, event ORDER BY vin, event");
        
        price_data.groupBy("make", "model");
        mileage_data.groupBy("year");
        price_data.repartition(1).write().format("csv").save(outputFilename + "/output_price");
        mileage_data.repartition(1).write().format("csv").save(outputFilename + "/output_mileage");
        event_data.repartition(1).write().format("csv").save(outputFilename + "/output_event");
        spc.stop();
    }
}