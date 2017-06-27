package com.corgi.common

class CommonUtils {
  
}

/**
 * Define the mappin from Scala data type to Spark SQL data type
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
object CommonUtils {
  
  import org.apache.spark.sql.types._
    
  def apply(fieldType:String):DataType = {
     fieldType match {
       case "Byte" => ByteType
       case "Short" => ShortType
       case "Int" => IntegerType
       case "Long" => LongType
       case "Float" => FloatType 
       case "Double" => DoubleType
//     case "BigDecimal" => DecimalType
       case "String" => StringType
       case "Array[Byte]" => BinaryType
       case "Boolean" => BooleanType
       case "Timestamp" => TimestampType
       case "Date" => DateType
//     case "Seq" => ArrayType 
     }
  }
}