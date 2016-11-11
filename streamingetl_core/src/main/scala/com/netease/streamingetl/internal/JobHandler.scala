package com.netease.streamingetl.internal

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.JobSet

/**
  * Created by hzyuemeng1 on 2016/11/11.
  */
abstract class JobHandler(sparkManager: SparkManager) {
  private val jobSets: java.util.Map[Time, JobSet] = new ConcurrentHashMap[Time, JobSet]
  def submitJobSet(job_type:String,app_Name:String,job:JobSet)
  def submitJob[U](job:Job)(body: => U)

}
class JobSet(jobs: Array[Job])