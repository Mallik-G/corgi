package com.corgi.xml

import scala.xml._
import org.apache.spark.sql.SparkSession
import java.io.InputStreamReader
import java.io.CharArrayReader

/**
 * XML file parser by scala
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class XMLParser(val xmlFilePath:String) {
    
  def getXMLRoot():Elem = {
     
     return XML.loadFile(xmlFilePath)
    
  }
  
  def getHDFSXMLRoot(spark:SparkSession):Elem = {
     
     val dfsXMLFile = spark.sparkContext.wholeTextFiles(xmlFilePath)     
     return XML.load(new CharArrayReader(dfsXMLFile.collect()(0)._2.toCharArray()))
  }
  
  //Search Functions
  def findNodesByName(node:NodeSeq,name:String):NodeSeq = {
    
    return node \ name
  }
  
  def findAttrByName(node:NodeSeq,name:String):NodeSeq ={
    
    val attributeName = "@" + name
    return node \ attributeName
   
  }
  
  def getNodeText(node:NodeSeq):String = {
    return node.text
  }
   
}
