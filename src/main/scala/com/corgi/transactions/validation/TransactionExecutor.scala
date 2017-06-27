package com.corgi.transactions.validation

import com.corgi.xml.ValidationXMLInterpreter
import scala.xml._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

/**
 * Execute the validation Transaction 
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class TransactionExecutor(val spark:SparkSession) {
  
  def executeTransaction(validationXMLInterpreter:ValidationXMLInterpreter, transactionNode:NodeSeq) = {
        
    val funcName = validationXMLInterpreter.getTransactionFunc(transactionNode)

    if(funcName.equals("input_output")) {
        val source = validationXMLInterpreter.getTransactionSrc(transactionNode)
        val target = validationXMLInterpreter.getTransactionTarget(transactionNode)
        inputOutputValidation(source, target)
    }
  }
    
  
   def inputOutputValidation(source:Option[String], target:Option[String]) = {
     
       // For implicit conversions like converting RDDs to DataFrames
       import spark.implicits._
 
     if(source.nonEmpty && target.nonEmpty) {
       
       val inputTableName = source.getOrElse("")
       val outputTableName = target.getOrElse("")
                
       val inputResults:DataFrame = spark.sql("SELECT '" + inputTableName + "' as Table_Name, " +  "count(*) as Amount FROM " + inputTableName)
       val outputResults:DataFrame = spark.sql("SELECT '" + outputTableName + "' as Table_Name, " +  "count(*) as Amount FROM " + outputTableName)
       

        
       var unionDataset = inputResults.union(outputResults)  
       unionDataset.show()
       
     }
   
  }
  
}