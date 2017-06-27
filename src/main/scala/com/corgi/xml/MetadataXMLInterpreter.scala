package com.corgi.xml
import scala.xml._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession

/**
 * Parse the Metadata XML file for CSV data
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class MetadataXMLInterpreter(val xmlFilePath:String,val spark:SparkSession) {
  
    val xmlParser = new XMLParser(xmlFilePath)
    var xmlRoot:Elem = null
    if(xmlFilePath.contains("HDFS://") || xmlFilePath.contains("hdfs://")) {
      xmlRoot = xmlParser.getHDFSXMLRoot(spark)
    } else {
      xmlRoot = xmlParser getXMLRoot()
    }
    
    def getDelimiter():String = {
      
      val metaConfiguration = xmlParser.findNodesByName(xmlRoot, "configuration")
      return xmlParser.findAttrByName(metaConfiguration,"delimiter").toString()
      
    }
     
    def getColumns():ArrayBuffer[(String,String,String)] = {
          
     val columns =  xmlParser.findNodesByName(xmlRoot, "columns")
     val columnArray =  xmlParser.findNodesByName(columns, "column") 
     
     val arrayBuffer = new ArrayBuffer[(String,String,String)]()
      
     for(column <- columnArray) {
       val tuple = (xmlParser.getNodeText(column).trim(), xmlParser.findAttrByName(column,"type").toString(), xmlParser.findAttrByName(column,"nullable").toString())
       arrayBuffer += tuple
     }
     
     return arrayBuffer     
    }
    
}