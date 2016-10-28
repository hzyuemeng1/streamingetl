package com.netease.streamingetl

import java.util._


import com.netease.streamingetl.internal.Logging
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
  * Created by hzyuemeng1 on 2016/10/26.
  */
object streamingetlMain {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: streamingetlMain <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()
    val Array(brokerlist,topics) = args
    val conf = new SparkConf()
    val spark = SparkSession.builder().appName("streamingetl-demo").config(conf).enableHiveSupport().getOrCreate()
    val  ssc = StreamingContext.getActiveOrCreate(() => new StreamingContext(spark.sparkContext,Seconds(120)))

    // first,use we will be create a partition table use spark sql which hive also can see it

    spark.sql("CREATE TABLE user_partitioned (use String,money Double) PARTITIONED BY (city STRING,phone STRING);")

    // then we config dynamic partition to insert data which we receive from kafka and etl it

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("hive.exec.max.dynamic.partitions.pernode = 1000")
    spark.sql("hive.exec.max.dynamic.partitions=1000")

    //create data stream use spark to receive data from kafka
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokerlist, "serializer.class" -> "kafka.serializer.StringEncoder")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).repartition(200)
    val ds: DStream[String]= messages.map(_._2)

    // the etl the data to get the vauleable data we want,the sync to hive table we firsr create

    ds.foreachRDD{
      rdd => {
        val df = spark.read.json(rdd)
       // etl data
        df.write.insertInto("b") //receive data to tmp file
        // etl the data
        val tmp = spark.sql("select * from xxxx")
        // append data to hive table
        tmp.write.mode(SaveMode.Append).insertInto("c")
        // insert into hive table we create before

      }
    }




    val dstream =
    ssc.start()
    ssc.awaitTermination()
  }
}




/** Utility functions for Spark Streaming examples. */
object StreamingExamples extends Logging {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}