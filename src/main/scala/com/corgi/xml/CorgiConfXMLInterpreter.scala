package com.corgi.xml
import scala.xml._

/**
 * Parse the Corgi configuration XML file
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class CorgiConfXMLInterpreter(val xmlFilePath: String) {

  val xmlParser = new XMLParser(xmlFilePath)
  val xmlRoot: Elem = xmlParser getXMLRoot ()

  def getPropertyValueByName(propertyName: String, defaultValue: String): String = {

    val properties = xmlParser.findNodesByName(xmlRoot, "property")
    for (property <- properties) {
      val name = xmlParser.findNodesByName(property, "name")
      if (name.text == propertyName) {
        return xmlParser.findNodesByName(property, "value").text
      }
    }
    return defaultValue
  }

  @deprecated
  def getDefaultFS(): String = {

    val defaultValue = "hdfs://localhost:9000"
    val propertyName = "fs.defaultFS"

    getPropertyValueByName(propertyName, defaultValue)
  }

  def getMaster(): String = {
    
    val defaultValue = "local"
    val propertyName = "spark.master"

    getPropertyValueByName(propertyName, defaultValue)
    
  }
  
    def getSparkSubmitMode(): String = {
    
    val defaultValue = "client"
    val propertyName = "spark.submit.deployMode"

    getPropertyValueByName(propertyName, defaultValue)
    
  }
  
  def getWarehouseLocation(): String = {

    val defaultValue = "hdfs://localhost:9000/spark-warehouse"
    val propertyName = "spark.sql.warehouse.dir"

    getPropertyValueByName(propertyName, defaultValue)

  }

  def getMetastoreDBLocation(): String = {

    val defaultValue = null
    val propertyName = "javax.jdo.option.ConnectionURL"

    getPropertyValueByName(propertyName, defaultValue)

  }

  def getAppName(): String = {

    val defaultValue = "Corgi"
    val propertyName = "spark.app.name"

    getPropertyValueByName(propertyName, defaultValue)
  }

  def getDriverCores(): String = {

    val defaultValue = "1"
    val propertyName = "spark.driver.cores"

    getPropertyValueByName(propertyName, defaultValue)
  }

  def getDriverMemory(): String = {

    val defaultValue = "1g"
    val propertyName = "spark.driver.memory"

    getPropertyValueByName(propertyName, defaultValue)
  }
  
   def getExecutorMemory(): String = {

    val defaultValue = "1g"
    val propertyName = "spark.executor.memory"

    getPropertyValueByName(propertyName, defaultValue)
  }
   
   def getExecutorCores(): String = {

    val defaultValue = "3"
    val propertyName = "spark.executor.cores"

    getPropertyValueByName(propertyName, defaultValue)
  }
   
   def getExecutorInstances(): String = {

    val defaultValue = "3"
    val propertyName = "spark.executor.instances"

    getPropertyValueByName(propertyName, defaultValue)
  }
  
   def getYarnQueue(): String = {

    val defaultValue = "default"
    val propertyName = "spark.yarn.queue"

    getPropertyValueByName(propertyName, defaultValue)
  }
   
  def getAutoBroadcastJoinThreshold(): String = {

    val defaultValue = "10485760"
    val propertyName = "spark.sql.autoBroadcastJoinThreshold"

    getPropertyValueByName(propertyName, defaultValue)
  } 
   

}