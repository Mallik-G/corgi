package com.corgi.filter
import org.apache.spark.sql.DataFrame

/**
 * Parse the Column name in "select" clause
 * When using that Column as the parameter to pass to the Hive function 
 * and assign the return value from that function a new column name
 * 
 * @creator: Minghao.Yang
 * @date: 2017.05.20
 * @version 0.0.1
 */
class ColumnNameFilter {
  
   def filter(tableDF:DataFrame, filterType:String="default"):DataFrame = {
     
     if(filterType == "origin") {
       return originColumnNameFilter(tableDF)
     } else {
       return defaultFilter(tableDF)
     }
     
     tableDF
   }
   
   def defaultFilter(tableDF:DataFrame):DataFrame = {
     
      var filteredTableDF = tableDF
      val columns = tableDF.columns
      var attributeCode = 0
      for(column <- columns) {       
        if(column.contains("(") && column.contains(")")) {

           filteredTableDF = filteredTableDF.withColumnRenamed(column, "__attr" + attributeCode + "__")
           attributeCode += 1
        }
     }
     return filteredTableDF 
   }
   
   def originColumnNameFilter(tableDF:DataFrame):DataFrame = {
     
      var filteredTableDF = tableDF
      val columns = tableDF.columns
      for(column <- columns) {       
        if(column.contains("(") && column.contains(")")) {
          
          var originColumnName = column.substring(column.lastIndexOf("(")+1,column.indexOf(")"))
          if(originColumnName.contains(",")) {
             originColumnName = originColumnName.split(",")(0)
          }  
          filteredTableDF = filteredTableDF.withColumnRenamed(column, originColumnName)
        }
     }
     filteredTableDF     
   }
   
}