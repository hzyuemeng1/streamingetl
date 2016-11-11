package org.apache.spark.sql


import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.streamingetl.execution.command.{DropDatabaseCommand, CreateDataSourceTableCommand, CreateDatabaseCommand}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
  * Created by hzyuemeng1 on 2016/10/25.
  */
class persiteDB(spark:SparkSession,schema:Option[StructType]) {


  def saveSchmaToDB(table: String,partition_fileds: Array[String] = Array.empty[String]) = {

    CreateDataSourceTableCommand(
      TableIdentifier(table),
      schema,
      "json",
      Map.empty[String,String],
      partition_fileds,
      None,
      false,
      true).run(spark)
  }

  def getSchemaFromTable(table_name: String):Option[StructType] = {
    val sma = spark.sqlContext.tables(table_name).schema
     sma match {
      case st:StructType if st != null  =>
        Some(sma)
      case _ => None
    }
  }

  def createDB(dbname:String): Unit ={
    CreateDatabaseCommand(dbname,
      ifNotExists = true,
      None,
      None,
      Map.empty[String,String]).run(spark)
  }
  def dropDB(dbname:String) = {
        DropDatabaseCommand(
          dbname,
          ifExists = false,
          cascade = false).run(spark)
  }



  def saveETLLoigcal(val sql:String)

}
class eltContainer(appName:String){

  def submitJob(job_type:String,app_Name:String,job:Job)
}