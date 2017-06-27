package com.corgi.xml
import scala.xml._

/**
 * Parse the Main Channel XML file
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class MainChannelXMLInterpreter(val xmlFilePath:String) {
  
   val xmlParser = new XMLParser(xmlFilePath)
   val xmlRoot:Elem = xmlParser getXMLRoot()
   
   def getChannels():NodeSeq = {
    
     val channels = xmlParser.findNodesByName(xmlRoot, "channels")    
     return xmlParser.findNodesByName(channels, "channel")
        
  }
  
  def getChannelFilePath(channelNode:NodeSeq):String = {
    
    val channelFolderPath = xmlFilePath.toString().substring(0, xmlFilePath.toString().lastIndexOf("/"))    
    return channelFolderPath + "/" + xmlParser.getNodeText(channelNode).trim()
    
  }
  
}