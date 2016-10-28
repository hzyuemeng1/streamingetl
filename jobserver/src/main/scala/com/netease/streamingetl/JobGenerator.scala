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
import org.apache.spark.streaming.Time

import scala.util.{Failure, Success, Try}



/** Event classes for JobGenerator */
private[streamingetl] sealed trait JobGeneratorEvent
private[streamingetl] case class GenerateJobs(time: Time) extends JobGeneratorEvent
private[streamingetl] case class ClearMetadata(time: Time) extends JobGeneratorEvent
private[streamingetl] case class DoCheckpoint(
    time: Time, clearCheckpointDataLater: Boolean) extends JobGeneratorEvent
private[streamingetl] case class ClearCheckpointData(time: Time) extends JobGeneratorEvent

/**
 * This class generates jobs from DStreams as well as drives checkpointing and cleaning
 * up DStream metadata.
 */
private[streamingetl]
class JobGenerator(jobScheduler: JobServer) extends Logging {




  // eventLoop is created when generator starts.
  // This not being null means the scheduler has been started and not stopped
  private var eventLoop: EventLoop[JobGeneratorEvent] = null



  /** Start generation of jobs */
  def start(): Unit = synchronized {
    if (eventLoop != null) return // generator has already been started



    eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
      override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = {
        jobScheduler.reportError("Error in job generator", e)
      }
    }
    eventLoop.start()

  }

  /**
   * Stop generation of jobs. processReceivedData = true makes this wait until jobs
   * of current ongoing time interval has been generated, processed and corresponding
   * checkpoints written.
   */
  def stop(processReceivedData: Boolean): Unit = synchronized {
    if (eventLoop == null) return // generator has already been stopped

    eventLoop.stop()
    eventLoop = null
     logInfo("Stopped JobGenerator")
  }


  /** Processes all events */
  private def processEvent(event: JobGeneratorEvent) {
    logDebug("Got event " + event)
    event match {
      case _ =>
    }
  }


  /** Generate jobs and perform checkpointing for the given `time`.  */
  private def generateJobs(time: Time) {

    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
  }

  /** Clear DStream metadata for the given `time`. */
  private def clearMetadata(time: Time) {

  }


}
