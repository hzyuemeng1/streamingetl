package com.netease.streamingetl.flink.table

import java.util.{HashMap, Properties}


import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import org.apache.sling.commons.json.JSONObject

import scala.util.Random

/**
  * Created by hzyuemeng1 on 2016/10/27.
  */
object KafkaProudcer {
  def main(args: Array[String]) {
    var topic = "test_json"
    var brokerList = "db-180.photo.163.org:9092"
    var interval :Long= 500
    if (args.length != 0) {
      val Array(topic,brokerList,interval) = args
    }

    val props1 = new Properties()
    props1.put("metadata.broker.list", brokerList)
    props1.put("serializer.class", "kafka.serializer.StringEncoder")
    props1.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props1.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val kafkaConfig = new ProducerConfig(props1)
    val producer = new Producer[String,String](kafkaConfig)


    while(true) {
      // prepare event data
      val event = getJson

      // produce event message
      producer.send(new KeyedMessage[String, String](topic, event.toString))
      println("Message sent: " + event)

      Thread.sleep(interval)
    }
  }


  def getJson:JSONObject = {

    import scala.collection.JavaConverters._

    val city = Array[String]("beijing","berlin","tokyo","newyork","hangzhou")
    val sed = new Random
    val city_name = city(sed.nextInt(city.length))
    val user_name = "user" + sed.nextInt(100)
    val ip = sed.nextInt(1000)
    val hostname = "host" + sed.nextInt(100)
    val money = sed.nextFloat()
    val jObj = new JSONObject
    jObj.put("ip",ip)
    jObj.put("hostname",hostname)
    val phone_type = Array[String]("huawei","apple","xiaomi","oppo","vivio","other")
    val phone = phone(sed.nextInt(phone_type.length))
    val body = Map("user" -> user_name, "city" -> city_name,"money" -> money,"phone" ->phone)
    jObj.put("body",body.asJava)
    jObj
  }
}


