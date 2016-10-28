package com.netease.streamingetl.schema

import org.apache.sling.commons.json.JSONObject
import org.apache.spark.sql.{DataFrame, SampleData, SparkSession}
import org.apache.spark.sql.types.StructType

/**
  * Created by hzyuemeng1 on 2016/10/24.
  *
  */

class JsonSchemaGenerator(sparksession:SparkSession) extends generatorSchema{
  override var schema: StructType = _

  val func = (ishow:Boolean,df: DataFrame) => {
    ishow match {
      case true => {
        sampleData = SampleData(df)
      }
      case _  => {
        sampleData
      }
    }
  }

  override def generateSchema(infer: inferInformation) = {
    infer match {
      case inferInformation(true,isShow,path) => {
        val df = sparksession.read.json(path)
        schema = df.schema

        func(isShow,df)

      }

      case inferInformation(false,isShow,jsonString) => {
        val rdd = sparksession.sparkContext.makeRDD(Seq(jsonString),8)
        val df = sparksession.read.json(rdd)

        func(isShow,df)
    }
    }

  }

  def getShowData:String = sampleData
  def getSchema: StructType = schema


}
