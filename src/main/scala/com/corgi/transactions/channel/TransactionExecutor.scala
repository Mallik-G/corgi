package com.corgi.transactions.channel
import org.apache.spark.sql.SparkSession
import com.corgi.filter.ColumnNameFilter
import org.apache.spark.sql.SaveMode

/**
 * Execute the SQL in the transaction and register the results into hive table or as a temporary table
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class TransactionExecutor(val spark:SparkSession) {
  
   def executeTransaction(SQL:String, tableName:String, persistentStatus:String) = {
    
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
     
    var resultsDF = spark.sql(SQL)  
    val columnNameFilter = new ColumnNameFilter()
    resultsDF = columnNameFilter.filter(resultsDF,"origin")
    resultsDF.createOrReplaceTempView(tableName)
    if(persistentStatus == "false") {
      resultsDF.createOrReplaceTempView(tableName)
    } else {
      resultsDF.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable(tableName)
    }
          
    val results = spark.sql("SELECT * FROM " + tableName + " LIMIT 100")      
//    results.map(attributes => "Name: " + attributes(0) + " age: " + attributes(1) + 
//        " gender: " + attributes(2) + " occupation: " + attributes(3)).show()
    results.show()

  }
  
}