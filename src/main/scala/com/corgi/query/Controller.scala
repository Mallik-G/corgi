package com.corgi.query

import org.apache.spark.sql.SparkSession
import scala.xml._
import com.corgi.xml.CorgiConfXMLInterpreter
import org.apache.log4j.{Level, Logger}

/**
 * Query the Transaction result by SQL
 * Every project has its own warehouse and metastore to store the transaction info, 
 * so project path is additional parameter when do the query
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class Controller {
  
   def executeQuery(SQL:String,spark:SparkSession)= {
    
     var resultsDF = spark.sql(SQL)
     resultsDF.show()
     
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
         
//       val variableNum = args.size
//       var SQL = ""
//       for(i <- 1 to variableNum-1) {
//         SQL += (args(i) + " ")
//       }
         
       val SQL = args(1)
       val controller = new Controller() 
       controller.executeQuery(SQL.trim(),spark)
     
      
   }
}