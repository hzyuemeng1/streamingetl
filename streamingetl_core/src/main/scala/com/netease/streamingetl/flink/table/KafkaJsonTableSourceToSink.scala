package com.netease.streamingetl.flink.table

import java.util.Properties

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.table.{TableEnvironment, Row}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner

/**
  * Created by hzyuemeng1 on 2016/10/19.
  */
object KafkaJsonTableSourceToSink {
  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Missing parameters!\n Usage: KafkaJsonTableSourceToSink --sourceTopic <source topic> "
        + "--sinkTopic <sink topic> --brokerList <kafka brokers> --zkConenection <zk quorum> --gropuId <group id>")
      return
    }
   val Array(sourceTopic,sinkTopic,brokerList,zkConnection,groupId) = args
   val fieldNames = Seq("name","id","address")
   val  typeInfo = Array[TypeInformation[_]](
     BasicTypeInfo.STRING_TYPE_INFO,
     BasicTypeInfo.BIG_INT_TYPE_INFO,
     BasicTypeInfo.STRING_TYPE_INFO)
   val kafkaConfig = createKafkaProps(zkConnection,brokerList,groupId)
   val kJsonInstances = KafkaJsonTableSourceToSink(sinkTopic, sinkTopic, kafkaConfig,fieldNames.toArray,typeInfo)
   val sqlQuery = "SELECT STREAM name, id AS user_id, address AS city " + "FROM testKafkaJson " + "WHERE address LIKE 'hangzhou'"
   val tableSource = kJsonInstances.getKafkaTableSource
   val tableEnv = getTableEnv
    tableEnv.registerTableSource("testKafkaJson",tableSource)
   //val tableSink = kJsonInstances.getKafkaTableSink
   val hangzhou = tableEnv.sql(sqlQuery)
    //hangzhou.writeToSink(tableSink)

  }
  def getTableEnv = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(senv)
    tEnv
  }

  def apply(sourceTopic: String, sinkTopic: String, properties: Properties, fieldsNames: Array[String], typeInfo: Array[TypeInformation[_]]) = {
    new KafkaJsonTableSourceToSink(sourceTopic, sinkTopic, properties, fieldsNames, typeInfo)
  }

  private def createKafkaProps(zookeeperConnect: String, bootstrapServers: String, groupId: String): Properties = {
    val props: Properties = new Properties
    props.setProperty("zookeeper.connect", zookeeperConnect)
    props.setProperty("bootstrap.servers", bootstrapServers)
    props.setProperty("group.id", groupId)
    props
  }

}

class KafkaJsonTableSourceToSink(
      val sourceTopic: String,
      val sinkTopic: String,
      val properties: Properties,
      val fieldsNames: Array[String],
      val typeInfo: Array[TypeInformation[_]]) {

  def getKafkaTableSource: Kafka09JsonTableSource = {
    new Kafka09JsonTableSource(sourceTopic, properties, fieldsNames, typeInfo)
  }
  /*def getKafkaTableSink: Kafka09JsonTableSink = {
    new Kafka09JsonTableSink(sinkTopic,properties,new FixedPartitioner[Row])
  }*/
}

