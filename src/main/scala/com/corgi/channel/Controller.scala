package com.corgi.channel
import com.corgi.xml.ChannelXMLInterpreter
import com.corgi.xml.CorgiConfXMLInterpreter
import com.corgi.xml.MainChannelXMLInterpreter
import com.corgi.source.CSVSource
import com.corgi.source.ParquetSource
import com.corgi.transactions.channel.TransactionExecutor
import org.apache.spark.sql.SparkSession
import com.corgi.sink.CSVSink
import com.corgi.sink.ParquetSink
import scala.xml._
import com.corgi.xml.XMLParser
import org.apache.log4j.{ Level, Logger }

/**
 * Parse and execute Channel file
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class Controller() {
  
  /**
	 * Execute Channel file
	 * 
	 * @creator: Minghao.Yang
	 * @createDate: 2017.05.20
	 * @modifier:
	 * @modifiedDate:
	 * @param channelXmlPath Channel file path
	 * @param spark SparkSession Object
	 * @return
	 * @throws
	 */
  def executeChannel(channelXmlPath: String, spark: SparkSession) = {

    val channelXMLInterpreter = new ChannelXMLInterpreter(channelXmlPath)

    executeSource(channelXMLInterpreter, spark)
    executeTransactions(channelXMLInterpreter, spark)
    executeSink(channelXMLInterpreter, spark)

  }
  
   /**
	 * Execute Source part in Channel file
	 * 
	 * @creator: Minghao.Yang
	 * @createDate: 2017.05.20
	 * @modifier:
	 * @modifiedDate:
	 * @param channelXMLInterpreter ChannelXMLInterpreter Object
	 * @param spark SparkSession Object
	 * @return
	 * @throws
	 */
  def executeSource(channelXMLInterpreter: ChannelXMLInterpreter, spark: SparkSession) = {

    //execute source
    val sources = channelXMLInterpreter.getSources()
    for (source <- sources) {

      val sourceDataPath = channelXMLInterpreter.getSourceDataPath(source)
      val souceDataType = channelXMLInterpreter.getSourceDataType(source)
      val souceTableName = channelXMLInterpreter.getSourceName(source)

      if (souceDataType == "csv") {

        val metaDataPath = channelXMLInterpreter.getMetaDataPath(source)
        val csvSource = new CSVSource(spark)
        csvSource.registerByCSV(sourceDataPath, metaDataPath, souceTableName)

      } else if(souceDataType == "parquet") {
        
        val parquetSource = new ParquetSource(spark)
        parquetSource.registerByParquet(sourceDataPath, souceTableName)
        
      }
    }
  }
  
  /**
	 * Execute Transactions part in Channel file
	 * 
	 * @creator: Minghao.Yang
	 * @createDate: 2017.05.20
	 * @modifier:
	 * @modifiedDate:
	 * @param channelXMLInterpreter ChannelXMLInterpreter Object
	 * @param spark SparkSession Object
	 * @return
	 * @throws
	 */
  def executeTransactions(channelXMLInterpreter: ChannelXMLInterpreter, spark: SparkSession) = {
    
    //execute transaction
    val transactions = channelXMLInterpreter.getTransactions()
    val transactionExecutor = new TransactionExecutor(spark)
    for (transaction <- transactions) {

      val SQL = channelXMLInterpreter.getTransactionContents(transaction)
      val tableName = channelXMLInterpreter.getTransactionName(transaction)
      val persistentStatus = channelXMLInterpreter.getTransactionPersistentStatus(transaction) match {
        case Some("false") => "false"
        case _             => "true"
      }

      transactionExecutor.executeTransaction(SQL.trim(), tableName.trim(), persistentStatus)
    }
  }
  
  /**
	 * Execute Sink part in Channel file
	 * 
	 * @creator: Minghao.Yang
	 * @createDate: 2017.05.20
	 * @modifier:
	 * @modifiedDate:
	 * @param channelXMLInterpreter ChannelXMLInterpreter Object
	 * @param spark SparkSession Object
	 * @return
	 * @throws
	 */
  def executeSink(channelXMLInterpreter: ChannelXMLInterpreter, spark: SparkSession) = {
    //execute sink 
    val sinks = channelXMLInterpreter.getSinks()
    for (sink <- sinks) {

      val sinkDataName = channelXMLInterpreter.getSinkName(sink)
      val sinkTransactionName = channelXMLInterpreter.getSinkTransactionName(sink)
      val sinkDataPath = channelXMLInterpreter.getSinkPath(sink)
      val sinkDataType = channelXMLInterpreter.getSinkType(sink)
      

      if (sinkDataType == "csv") {
        
        val csvSink = new CSVSink(spark)
        csvSink.sinkToCSV(sinkDataName, sinkTransactionName, sinkDataPath)
        
      } else if(sinkDataType == "parquet") {
        
        val parquetSink = new ParquetSink(spark)
        parquetSink.sinkToParquet(sinkDataName, sinkTransactionName, sinkDataPath)
        
      }
    }
  }

}

/**
 * Initiate Spark Session Object and execute all the Channel file in sequence defined in main.cha.xml file
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
object Controller {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {

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
    val autoBroadcastJoinThreshold = corgiConfXMLInterpreter.getAutoBroadcastJoinThreshold()

    val spark = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + metastoreDBLocation + ";create=true")
      .config("spark.driver.cores", driverCores)
      .config("spark.driver.memory", driverMemory)
      .config("spark.executor.memory", executorMemory)
      .config("spark.executor.cores", executorCores)
      .config("spark.executor.instances", executorInstances)
      .config("spark.yarn.queue", yarnQueue)
      .config("spark.submit.deployMode", submitMode)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.autoBroadcastJoinThreshold",autoBroadcastJoinThreshold)
      .enableHiveSupport()
      .getOrCreate()

    val mainChannelXMLInterpreter = new MainChannelXMLInterpreter(args(1))
    val channels = mainChannelXMLInterpreter.getChannels()
    for (channel <- channels) {
      val channelFileName = mainChannelXMLInterpreter.getChannelFilePath(channel)
      val controller = new Controller()
      controller.executeChannel(channelFileName, spark)
    }

  }
}