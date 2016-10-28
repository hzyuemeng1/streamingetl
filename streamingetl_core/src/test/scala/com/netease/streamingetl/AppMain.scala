package com.netease.streamingetl

import com.netease.streamingetl.schema.{inferInformation, JsonSchemaGenerator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by hzyuemeng1 on 2016/10/24.
  */
object AppMain {
  def main(args: Array[String]) {
   // val jsonstr =
    val conf = new SparkConf().setMaster("local[20]")
    val sparksession = SparkSession.builder().config(conf).getOrCreate()
    val jsonGenerator = new JsonSchemaGenerator(sparksession)
    println(jsonGenerator.generateSchema(new inferInformation(true,true,"D:/Dev_Tools/data/people.json")))
  }

}
