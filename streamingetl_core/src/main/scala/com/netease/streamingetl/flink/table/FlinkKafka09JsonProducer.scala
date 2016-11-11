package com.netease.streamingetl.flink.table

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, DataStream}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer08, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.{TypeInformationSerializationSchema, JSONDeserializationSchema}
import org.apache.sling.commons.json.JSONObject

import scala.util.Random

/**
  * Created by hzyuemeng1 on 2016/10/20.
  */
object FlinkKafka09JsonProducer {
  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Missing parameters!\n Usage: Kafka09JsonProducer"
        + "--topic <sink topic> --brokerlist <kafka brokers>")
    }
    val parametersTool = ParameterTool.fromArgs(args)
    val brokerList = parametersTool.getRequired("brokerlist")
    val topic = parametersTool.getRequired("topic")
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.getConfig.disableSysoutLogging
    senv.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 1000))
    implicit val typeInfo = TypeInformation.of(classOf[JSONObject])
    val source: DataStream[JSONObject] = senv.addSource(new SourceFunction[JSONObject] {
      var running = true

      override def run(ctx: SourceContext[JSONObject]): Unit = {
        while (running) {
          try {
            val obj = getJson
            ctx.collect(obj)
            println(s"Messages: $obj") //get json object
            Thread.sleep(5000)
          } catch {
            case ex:Throwable =>
              ex.getStackTrace
          }
        }
      }

      override def cancel(): Unit = {
        running = false
      }
    }

    )
    println("start to execute *****")
    source.addSink(new FlinkKafkaProducer09[JSONObject](brokerList,topic,new TypeInformationSerializationSchema[JSONObject](typeInfo,new ExecutionConfig())))


    senv.execute("write json data to kafka")



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
    val phone  = phone_type(sed.nextInt(phone_type.length))
    val body = Map("user" -> user_name, "city" -> city_name,"money" -> money,"phone" ->phone)
    jObj.put("body",body.asJava)
    jObj
  }
}
