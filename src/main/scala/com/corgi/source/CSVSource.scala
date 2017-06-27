package com.corgi.source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer
import com.corgi.common.CommonUtils
import com.corgi.xml.MetadataXMLInterpreter

/**
 * Read the CSV data from storage medium and register them into as a table
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class CSVSource(val spark:SparkSession) {
  
  def registerByCSV(csvPath:String, metaDataPath:String, tableName:String) = {
         
     // For implicit conversions like converting RDDs to DataFrames
     import spark.implicits._
              
     val tableRDD = spark.sparkContext.textFile(csvPath)
     
     import org.apache.spark.sql.types._
     
     val metadataXMLInterpreter = new MetadataXMLInterpreter(metaDataPath,spark)
     val columns = metadataXMLInterpreter.getColumns()
     val fields = columns.map(columnTuple =>
       if(columnTuple._3 == "true") {
         StructField(columnTuple._1, CommonUtils(columnTuple._2), nullable = true)
       } else {
         StructField(columnTuple._1, CommonUtils(columnTuple._2), nullable = false)
       })
          
//   val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
     val schema = StructType(fields)
     
     // Convert records of the RDD (people) to Rows
     val delimiter = metadataXMLInterpreter.getDelimiter()
     val escaped_delimiter = delimiter match {
       case "||" => "\\|\\|"
       case "|" => "\\|"
       case _ => delimiter
     }
     val rowRDD = tableRDD
        .map(_.split(escaped_delimiter))
        .filter(fields.size == _.size) //filter the line doesn't contain sufficient columns
        .map(attributes => Row.fromSeq(attributes))
        
     val tableDF = spark.createDataFrame(rowRDD, schema)
     tableDF.createOrReplaceTempView(tableName)            
  }
  
}