package com.netease.streamingetl.schema

import org.apache.spark.sql.types.StructType

/**
  * Created by hzyuemeng1 on 2016/10/24.
  *
  */
case class inferInformation(isJson: Boolean,showSample:Boolean,content: String)

trait generatorSchema {

  var schema:StructType
  val typeInfo = "json"
  var sampleData = ""
  def init(schema:StructType) = {
    this.schema = schema
  }

  def generateSchema(content:inferInformation)

}
