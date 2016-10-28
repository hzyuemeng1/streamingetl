package com.netease.streamingetl.db

import com.netease.streamingetl.internal.Logging
import com.netease.streamingetl.schema.JsonSchemaGenerator
import org.apache.spark.sql.{SchemaType, SparkSession}

/**
  * Created by hzyuemeng1 on 2016/10/25.
  */
class UpdateSchema(spark: SparkSession) extends JsonSchemaGenerator(spark) with  Logging{

   self =>

  val updateSchema = new SchemaType(schema)
  val getModifySchema = updateSchema.getSchema







}
