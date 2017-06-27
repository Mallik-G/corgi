package com.corgi.validation

import com.corgi.xml.MainValidationXMLInterpreter
import com.corgi.xml.CorgiConfXMLInterpreter
import com.corgi.xml.ValidationXMLInterpreter
import com.corgi.transactions.validation.TransactionExecutor
import org.apache.spark.sql.SparkSession
import scala.xml._
import com.corgi.xml.XMLParser
import org.apache.log4j.{Level, Logger}

/**
 * Parse and execute Validation file
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class Controller {
  
   def executeValidation(validationXmlPath:String,spark:SparkSession)= {
    
     val validationXMLInterpreter = new ValidationXMLInterpreter(validationXmlPath)
             
     //execute transaction
     val transactions = validationXMLInterpreter.getTransactions()   
     val transactionExecutor = new com.corgi.transactions.validation.TransactionExecutor(spark)
     for(transaction <- transactions) {               
        transactionExecutor.executeTransaction(validationXMLInterpreter,transaction)      
     }  
  } 
}

object Controller {
  
   
   Logger.getLogger("org").setLevel(Level.ERROR)
  
   def main(args:Array[String]) {
     
     val corgiConfXMLInterpreter = new CorgiConfXMLInterpreter(args(0))
     
     //initiate spark-sql env
     val warehouseLocation = corgiConfXMLInterpreter.getWarehouseLocation()
     val metastoreDBLocation = corgiConfXMLInterpreter.getMetastoreDBLocation()
     val master = corgiConfXMLInterpreter.getMaster()
     val appName = corgiConfXMLInterpreter.getAppName()
     val driverCores = corgiConfXMLInterpreter.getDriverCores()
     val driverMemory = corgiConfXMLInterpreter.getDriverMemory()
     val executorMemory = corgiConfXMLInterpreter.getExecutorMemory()
     val executorCores = corgiConfXMLInterpreter.getExecutorCores()
     val executorInstances = corgiConfXMLInterpreter.getExecutorInstances()
     val yarnQueue = corgiConfXMLInterpreter.getYarnQueue()
     val submitMode = corgiConfXMLInterpreter.getSparkSubmitMode()
     
     val spark = SparkSession
         .builder()
         .master(master)
         .appName(appName)
         .config("spark.sql.warehouse.dir", warehouseLocation)
         .config("javax.jdo.option.ConnectionURL","jdbc:derby:;databaseName=" + metastoreDBLocation +";create=true")
         .config("spark.driver.cores",driverCores)
         .config("spark.driver.memory",driverMemory)
         .config("spark.executor.memory",executorMemory)
         .config("spark.executor.cores",executorCores)
         .config("spark.executor.instances",executorInstances)
         .config("spark.yarn.queue",yarnQueue)
         .config("spark.submit.deployMode",submitMode)
         .enableHiveSupport()
         .getOrCreate()
     
     val mainValidationXMLInterpreter = new MainValidationXMLInterpreter(args(1))
     val validations = mainValidationXMLInterpreter.getValidations()
     for(validation <- validations) {
       val validationFileName = mainValidationXMLInterpreter.getValidationFilePath(validation)
       val controller = new Controller() 
       controller.executeValidation(validationFileName,spark)
     }
      
   }
}