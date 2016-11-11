package com.netease.streamingetl.internal

import scala.collection.immutable.HashMap

/**
  * Created by hzyuemeng1 on 2016/11/11.
  */
abstract class Job[T] (jobID: String){
  val jobConf:HashMap[String,T]
}
