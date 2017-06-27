package com.corgi.xml
import scala.xml._

/**
 * Parse the Main Validation configuration XML file
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class MainValidationXMLInterpreter(val xmlFilePath:String) {
  
   val xmlParser = new XMLParser(xmlFilePath)
   val xmlRoot:Elem = xmlParser getXMLRoot()
   
   def getValidations():NodeSeq = {
    
     val validations = xmlParser.findNodesByName(xmlRoot, "validations")    
     return xmlParser.findNodesByName(validations, "validation")
        
  }
  
  def getValidationFilePath(validationNode:NodeSeq):String = {
    
    val validationFolderPath = xmlFilePath.toString().substring(0, xmlFilePath.toString().lastIndexOf("/"))    
    return validationFolderPath + "/" + xmlParser.getNodeText(validationNode).trim()
    
  }
  
}