/*
 * Copyright (2021) The Hyperspace Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.hyperspace

import scala.collection.JavaConverters._

import org.apache.iceberg.{PartitionSpec => IcebergPartitionSpec, Table, TableProperties}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.DataFrame

trait IcebergTestUtils {
  def createIcebergTable(dataPath: String, sourceDf: DataFrame): Table = {
    val props = Map(TableProperties.WRITE_NEW_DATA_LOCATION -> dataPath).asJava
    val schema = SparkSchemaUtil.convert(sourceDf.schema)
    val part = IcebergPartitionSpec.builderFor(schema).build()
    new HadoopTables().create(schema, part, props, dataPath)
  }

  def createIcebergTableWithPartitions(
      dataPath: String,
      sourceDf: DataFrame,
      partCol: String): Table = {
    val props = Map(TableProperties.WRITE_NEW_DATA_LOCATION -> dataPath).asJava
    val schema = SparkSchemaUtil.convert(sourceDf.schema)
    val part = IcebergPartitionSpec.builderFor(schema).identity(partCol).build()
    new HadoopTables().create(schema, part, props, dataPath)
  }
}
