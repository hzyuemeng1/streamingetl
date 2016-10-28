package com.netease.streamingetl.akkahttp

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.{IncomingConnection, ServerBinding}
import akka.http.scaladsl.model.Uri.Path.Segment
import akka.http.scaladsl.server.Route
import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.netease.streamingetl.internal.Logging

import org.apache.spark._
import org.apache.spark.streaming._
import scala.concurrent.Future



/**
  * Created by hzyuemeng1 on 2016/10/18.
  */
object TestAkkaHttp extends App with SparkService with Logging{

  implicit val system = ActorSystem("akka-http-spark-streamingtest")
  implicit val mat = ActorMaterializer
  implicit val ec = system.dispatcher
  val host = "localhost"
  val restPort  = 8099
  val conf = new SparkConf().setMaster("local[*]").setAppName("TestWebApp").set("spark.akka.heartbeat.interval", "10s")
  val ssc = new StreamingContext(conf, Seconds(30))
  val ds = ssc.textFileStream("/home")
  ds.print()

  //val restSource: Source[IncomingConnection, Future[ServerBinding]] = Http().bindAndHandle(sparkRoutes,interface = host,port = restPort)

  val sparkRoutes: Route = {
    get {
      path("start") {
        complete {

          try {
            ssc.start()
            ssc.awaitTermination()
            HttpResponse(200)
          } catch {
            case ex: Throwable =>
              logDebug(ex.getMessage,ex)
              HttpResponse(StatusCodes.InternalServerError, entity = s"Error found for id")
          }
        }
      }
    } ~ get {
      path("stop") {
        complete{
          try {
            ssc.stop(true)
            HttpResponse(200)
          } catch {
            case ex:Throwable =>
              logError(ex.getMessage)
              HttpResponse(404)

          }
        }
      }
    }
  }




}
