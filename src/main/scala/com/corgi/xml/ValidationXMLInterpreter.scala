package com.corgi.xml
import scala.xml._

/**
 * Parse the Validation configuration XML file
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class ValidationXMLInterpreter(val xmlFilePath:String) {
  
  val xmlParser = new XMLParser(xmlFilePath)
  val xmlRoot:Elem = xmlParser getXMLRoot()
  
  
  def getTransactions():NodeSeq = {
    
    val transactions = xmlParser.findNodesByName(xmlRoot, "transactions")    
    return xmlParser.findNodesByName(transactions, "transaction")
        
  }
  
  def getTransactionName(transactionNode:NodeSeq):String = {
    return xmlParser.findAttrByName(transactionNode,"name").toString()
  }
  
  def getTransactionFunc(transactionNode:NodeSeq):String = {
    return xmlParser.findAttrByName(transactionNode,"function").toString()
  }
  
  def getTransactionSrc(transactionNode:NodeSeq):Option[String] = {
    return Some(xmlParser.findAttrByName(transactionNode,"source").toString())
  }
  
  def getTransactionTarget(transactionNode:NodeSeq):Option[String] = {
    return Some(xmlParser.findAttrByName(transactionNode,"target").toString())
  }
  
  @deprecated
  def getTransactionPersistentStatus(transactionNode:NodeSeq):Option[String] = {
    return Some(xmlParser.findAttrByName(transactionNode,"persistent").toString())
  }
  
  @deprecated
  def getTransactionContents(transactionNode:NodeSeq):String = {
    return xmlParser.getNodeText(transactionNode)
  }
  
}