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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

// Note: The definition of these commands are based on the ones described in
// https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL


/**
 * A command for users to remove a database from the system.
 *
 * 'ifExists':
 * - true, if database_name does't exist, no action
 * - false (default), if database_name does't exist, a warning message will be issued
 * 'cascade':
 * - true, the dependent objects are automatically dropped before dropping database.
 * - false (default), it is in the Restrict mode. The database cannot be dropped if
 * it is not empty. The inclusive tables must be dropped at first.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *    DROP DATABASE [IF EXISTS] database_name [RESTRICT|CASCADE];
 * }}}
 */
case class DropDatabaseCommand(
    databaseName: String,
    ifExists: Boolean,
    cascade: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sessionState.catalog.dropDatabase(databaseName, ifExists, cascade)
    Seq.empty[Row]
  }

  override val output: Seq[Attribute] = Seq.empty
}





























