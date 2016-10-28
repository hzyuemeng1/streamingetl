package com.netease.streamingetl.flink.table

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.Row
import org.apache.flink.streaming.connectors.kafka.{Kafka09JsonTableSource, Kafka09TableSource}
import org.apache.flink.streaming.util.serialization.DeserializationSchema

/**
  * Created by hzyuemeng1 on 2016/10/19.
  */
object KafkaTableUtils {
  /**
    *
    * @param topic
    * @param properties
    * @param fieldsNames
    * @param typeInfo
    * @return
    */

  def createKafka09TableSource(
      topic: String,
      properties: Properties,
      deserializationSchema: DeserializationSchema[Row],
      fieldsNames: Array[String],
      typeInfo: Array[TypeInformation[_]]) = {

    if (deserializationSchema != null) {
      new Kafka09TableSource(topic, properties, deserializationSchema, fieldsNames, typeInfo)
    } else {
      new Kafka09JsonTableSource(topic, properties, fieldsNames, typeInfo)
    }
  }

  /**
    *
    * @param topic
    * @param properties
    * @param deserializationSchema
    * @param fieldsNames
    * @param typeInfo
    * @return
    */
  def createKafka08TableSource(
      topic: String,
      properties: Properties,
      deserializationSchema: DeserializationSchema[Row],
      fieldsNames: Array[String],
      typeInfo: Array[TypeInformation[_]]) = {
    if (deserializationSchema != null) {
      new Kafka09TableSource(topic, properties, deserializationSchema, fieldsNames, typeInfo)
    } else {
      new Kafka09JsonTableSource(topic, properties, fieldsNames, typeInfo)
    }
  }

/*  /**
    *
    * @param topic
    * @param properties
    * @param partitioner
    * @return
    */
  def createKafka09TableSink(
      topic: String,
      properties: Properties,
      partitioner: KafkaPartitioner[Row]) = {
    new Kafka09JsonTableSink(topic, properties, partitioner)
  }

  /**
    *
    * @param topic
    * @param properties
    * @param partitioner
    * @return
    */
  def createKafka08TableSink(
      topic: String,
      properties: Properties,
      partitioner: KafkaPartitioner[Row]) = {
    new Kafka08JsonTableSink(topic, properties, partitioner)
  }*/


}
