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

package org.apache.spark.sql.streamingetl.execution.command

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.util.control.NonFatal

// Note: The definition of these commands are based on the ones described in
// https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL


/**
 * Drops a table/view from the metastore and removes it if it is cached.
 *
 * The syntax of this command is:
 * {{{
 *   DROP TABLE [IF EXISTS] table_name;
 *   DROP VIEW [IF EXISTS] [db_name.]view_name;
 * }}}
 */
case class DropTableCommand(
    tableName: TableIdentifier,
    ifExists: Boolean,
    isView: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    // If the command DROP VIEW is to drop a table or DROP TABLE is to drop a view
    // issue an exception.
    catalog.getTableMetadataOption(tableName).map(_.tableType match {
      case CatalogTableType.VIEW if !isView =>
        throw new AnalysisException(
          "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead")
      case o if o != CatalogTableType.VIEW && isView =>
        throw new AnalysisException(
          s"Cannot drop a table with DROP VIEW. Please use DROP TABLE instead")
      case _ =>
    })
    try {
      sparkSession.sharedState.cacheManager.uncacheQuery(
        sparkSession.table(tableName.quotedString))
    } catch {
      case NonFatal(e) => log.warn(e.toString, e)
    }
    catalog.refreshTable(tableName)
    catalog.dropTable(tableName, ifExists)
    Seq.empty[Row]
  }
}























