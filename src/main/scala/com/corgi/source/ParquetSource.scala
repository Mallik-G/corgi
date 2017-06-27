package com.corgi.source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

/**
 * Read the Parquet data from storage medium and register them into as a table
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class ParquetSource(val spark:SparkSession) {
  
  def registerByParquet(parquetPath:String, tableName:String) = {
         
     // For implicit conversions like converting RDDs to DataFrames
     import spark.implicits._
     
     val tableDF = spark.read.parquet(parquetPath)
     tableDF.createOrReplaceTempView(tableName) 
                     
  }
  
}