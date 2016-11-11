package com.netease.streamingetl.internal

import scala.collection.mutable

/**
  * Created by hzyuemeng1 on 2016/11/11.
  */
abstract class ETLContainer(appName: String) {
  val sourceTable:String
  val targetTable:String
  val partitionFields:Array[String]
  val etlMap:mutable.HashMap[String,mutable.HashMap[String,String]]
  def etlLogical(etl_container:ETLContainer) :String
  def saveETLLogical(appName:String,sql:String)
  def getETLLogicalFromPath(metaPath: String):String
}
