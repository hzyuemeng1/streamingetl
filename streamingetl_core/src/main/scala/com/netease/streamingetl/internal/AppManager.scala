package com.netease.streamingetl.internal

/**
  * Created by hzyuemeng1 on 2016/11/11.
  */
abstract class AppManager(appName:String) {

  def restartApplication(appName:String,metaPath:String):Unit
  def getSparkManager(mataPath:String):SparkManager
  def getEtlSqlLogical(appName:String,metaPath: String):ETLContainer
  def getDStreamManager(appName:String,metaPath: String)

}
