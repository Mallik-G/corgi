package com.corgi.xml
import scala.xml._

/**
 * Parse the Channel XML file
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class ChannelXMLInterpreter(val xmlFilePath:String) {
  
    val xmlParser = new XMLParser(xmlFilePath)
    val xmlRoot:Elem = xmlParser getXMLRoot()
  
  def getSources():NodeSeq = {
      
     return xmlParser.findNodesByName(xmlRoot, "source") 
  }
    
  def getSourceDataPath(source:NodeSeq):String = {
      
    return xmlParser.findAttrByName(source,"path").toString()
        
  }
    
  def getSourceDataType(source:NodeSeq):String = {
    
   
    return xmlParser.findAttrByName(source,"type").toString()
        
  }
   
  def getMetaDataPath(source:NodeSeq):String = {
    
//    val source = xmlParser.findNodesByName(xmlRoot, "source")    
    return xmlParser.findAttrByName(source,"meta_data").toString()
        
  }
  
   def getSourceName(source:NodeSeq):String = {
          
    return xmlParser.findAttrByName(source,"name").toString()
        
  }
  
  def getTransactions():NodeSeq = {
    
    val transactions = xmlParser.findNodesByName(xmlRoot, "transactions")    
    return xmlParser.findNodesByName(transactions, "transaction")
        
  }
  
  def getTransactionContents(transactionNode:NodeSeq):String = {
    return xmlParser.getNodeText(transactionNode)
  }
  
  def getTransactionName(transactionNode:NodeSeq):String = {
    return xmlParser.findAttrByName(transactionNode,"name").toString()
  }
  
  def getTransactionPersistentStatus(transactionNode:NodeSeq):Option[String] = {
    return Some(xmlParser.findAttrByName(transactionNode,"persistent").toString())
  }
  
  def getSinks():NodeSeq = {
      
     return xmlParser.findNodesByName(xmlRoot, "sink") 
  }
  
  def getSinkName(sink:NodeSeq):String = {
   
    return xmlParser.findAttrByName(sink,"name").toString()
  }
  
  def getSinkTransactionName(sink:NodeSeq):String = {
    
    return xmlParser.findAttrByName(sink,"transaction_name").toString()
  }
  
   def getSinkPath(sink:NodeSeq):String = {
  
    return xmlParser.findAttrByName(sink,"path").toString()
  }
   
  def getSinkType(sink:NodeSeq):String = {   
    return xmlParser.findAttrByName(sink,"type").toString()
  }
  
}