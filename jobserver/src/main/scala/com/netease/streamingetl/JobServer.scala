/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.streamingetl


import com.netease.streamingetl.internal.Logging
import com.netease.streamingetl.utils.EventLoop
import org.apache.spark.streaming.scheduler.ErrorReported





private[streamingetl] sealed trait JobSchedulerEvent
private[streamingetl] case class JobStarted(job: Job, startTime: Long) extends JobSchedulerEvent
private[streamingetl] case class JobCompleted(job: Job, completedTime: Long) extends JobSchedulerEvent
private[streamingetl] case class ErrorReported(msg: String, e: Throwable) extends JobSchedulerEvent


private[streamingetl]
class JobServer extends Logging {


  private var eventLoop: EventLoop[JobSchedulerEvent] = null

  def start(): Unit = synchronized {
    if (eventLoop != null) return // scheduler has already been started

    logDebug("Starting JobScheduler")
    eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler") {
      override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = reportError("Error in job scheduler", e)
    }
    eventLoop.start()


  }

  def stop(processAllReceivedData: Boolean): Unit = synchronized {
    if (eventLoop == null) return // scheduler has already been stopped
    logDebug("Stopping JobScheduler")


    eventLoop.stop()
    eventLoop = null

  }


  private def processEvent(event: JobSchedulerEvent) {
    try {
      event match {
        case JobStarted(job, startTime) => handleJobStart(job, startTime)
        case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
        case ErrorReported(m, e) => handleError(m, e)
      }
    } catch {
      case e: Throwable =>
        reportError("Error in job scheduler", e)
    }
  }

  private def handleJobStart(job: Job, startTime: Long) {

  }

  private def handleJobCompletion(job: Job, completedTime: Long) {

  }

  private def handleError(msg: String, e: Throwable) {

  }

  def reportError(msg: String, e: Throwable) {

  }

  def isStarted(): Boolean = synchronized {
    eventLoop != null
  }

  private class JobHandler(job: Job) extends Runnable with Logging {
    override def run(): Unit = ???
  }
}

private[streamingetl] object JobServer {

}
