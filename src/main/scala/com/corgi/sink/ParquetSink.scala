package com.corgi.sink
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

/**
 * Write the Transaction results to storage medium with Parquet format
 * and register that results as an external table for querying
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class ParquetSink(val spark:SparkSession) {
  
   def sinkToParquet(tableName:String, transaction_name:String, sinkPath:String) = { 
      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._
      spark.sql("drop table if exists " + tableName)
      
      spark.table(transaction_name).write.format("parquet").mode(SaveMode.Overwrite).option("path", sinkPath + tableName).saveAsTable(tableName)
                           
  }
  
}