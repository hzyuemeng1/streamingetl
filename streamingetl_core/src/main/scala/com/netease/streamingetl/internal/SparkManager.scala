package com.netease.streamingetl.internal

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * Created by hzyuemeng1 on 2016/11/11.
  */
class SparkManager(appName: String,batchTime:Long,etlConf:SparkConf) {



  val spark: SparkSession = setSpark


  val ssc: StreamingContext = setStreamingContext
  val sc: Option[SparkContext] = if (spark != null) Some(spark.sparkContext) else None
  val sqlContxt: Option[SQLContext] = if (spark != null) Some(spark.sqlContext) else None

  def setSpark = {
    SparkSession.builder()
      .appName(appName)
      .config(etlConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  def setStreamingContext: StreamingContext = {
    StreamingContext
      .getActiveOrCreate(() => new StreamingContext(sc.get,Seconds(batchTime)))
  }

  def start(): Unit = ssc.start()
  def remember(duration: Duration): Unit = ssc.remember(duration)
  def checkpoint(directory: String): Unit = ssc.checkpoint(directory)
  def awaitTermination(): Unit = ssc.awaitTermination()
  def awaitTerminationOrTimeout(timeout: Long): Boolean = ssc.awaitTerminationOrTimeout(timeout)
  def stop(stopSparkContext: Boolean): Unit = ssc.stop(stopSparkContext)
  def stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit = ssc.stop(stopSparkContext, stopGracefully)


  def self = this

  def getSparkContext = sc.get
  def getSQLContext = sqlContxt.get



}

object SparkManager {

  def apply(batchTime:Long,appName: String,conf: SparkConf): SparkManager = {
    new SparkManager(appName,batchTime,conf)
  }



}