package com.corgi.sink
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

/**
 * Write the Transaction results to storage medium with CSV format
 * and register that results as an external table for querying
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class CSVSink(val spark:SparkSession) {
  
    def sinkToCSV(tableName:String, transaction_name:String, sinkPath:String) = { 
      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._
      spark.sql("drop table if exists " + tableName)
      
      spark.table(transaction_name).write.format("csv").mode(SaveMode.Overwrite).option("delimiter","|").option("path", sinkPath + tableName).saveAsTable(tableName)
            
//    spark.table(transaction_name).write.partitionBy("age").format("csv").mode(SaveMode.Overwrite).option("delimiter","|").option("path", sinkPath + tableName).saveAsTable(tableName)
                 
//    spark.sql("create table " + tableName + " (name string,age string,gender string,occupation string) partitioned by (ages string) STORED AS TEXTFILE location '/Users/minghao/Desktop/杂七杂八/corgi/papa'")
//    spark.sql("insert overwrite table " + tableName + " partition(ages) select name,age,gender,occupation,age as ages from " + transaction_name)
//    spark.table(transaction_name).write.format("csv").mode(SaveMode.Overwrite).option("delimiter","|").option("path", sinkPath + "/realage=27").option("basePath", sinkPath).saveAsTable("tmp" + tableName)
               
  }
    
}