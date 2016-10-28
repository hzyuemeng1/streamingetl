package com.netease.streamingetl.akkahttp

import akka.http.scaladsl.model.{HttpEntity, ContentTypes}
import org.apache.spark.{SparkContext, SparkConf}
import akka.http.scaladsl.server.Directives._
/**
  * Created by hzyuemeng1 on 2016/10/18.
  */
trait SparkService {
  val sparkConf = new SparkConf().setAppName("spark-akka-http").setMaster("local")
  val sc = new SparkContext(sparkConf)
  val route =
    path("start") {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>Spark version in this template is: ${sc.version}</h1>"))
      }
    }


}
