package com.netease.streamingetl.internal

import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable._

/**
  * Created by hzyuemeng1 on 2016/11/11.
  *
  */
abstract class DStreamManager(sparkManager: SparkManager,properties:HashMap[String,String]) {

  def sourceDstream(props:HashMap[String,String]):DStream
  def transforms:HashMap[String,List[String]]
  def getDStreamConf(appName: String):HashMap[String,String]
}
