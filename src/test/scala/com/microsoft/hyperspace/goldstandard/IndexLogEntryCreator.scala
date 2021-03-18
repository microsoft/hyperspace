package com.microsoft.hyperspace.goldstandard
// scalastyle:off
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.example.SparkApp
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.util.{JsonUtils, PathUtils}

object IndexLogEntryCreator extends SparkApp {
  def createIndex(str: String): Unit = {
    val splits = str.split(";")
    val name = splits(0)
    val indexCols = splits(2).split(",").toSeq
    val includedCols = splits(3).split(",").toSeq
    val config = IndexConfig(name, indexCols, includedCols)
    val table = splits(1)
    val indexPath = {
      val baseResourcePath = {
        // use the same way as `SQLQueryTestSuite` to get the resource path
        java.nio.file.Paths.get("src", "test", "resources", "tpcds").toFile
      }
      PathUtils
        .makeAbsolute(
          new Path(baseResourcePath.toString, s"hyperspace/indexes/$name"),
          new Configuration)
    }
    new IndexLogManagerImpl(indexPath)
      .writeLog(0, toIndex(config, s"${indexPath.toString}/v__=0", table, spark))
  }

  def toRelation(sourceDf: DataFrame): Option[Relation] = {
    val leafPlans = sourceDf.queryExecution.optimizedPlan.collectLeaves()
    leafPlans.head match {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, dataSchema, _, _, _),
          _,
          _,
          _) =>
        val sourceDataProperties =
          Hdfs.Properties(Content.fromDirectory(location.rootPaths.head, new FileIdTracker))
        val fileFormatName = "parquet"

        Some(
          Relation(
            location.rootPaths.map(_.toString),
            Hdfs(sourceDataProperties),
            dataSchema.json,
            fileFormatName,
            Map.empty))
      case _ => None
    }
  }

  def toIndex(
      config: IndexConfig,
      path: String,
      tableName: String,
      spark: SparkSession): IndexLogEntry = {
    val sourceDf = spark.table(tableName)
    val indexSchema = {
      val allCols = config.indexedColumns ++ config.includedColumns
      StructType(sourceDf.schema.filter(f => allCols.contains(f.name)))
    }
    val relation: Relation = toRelation(sourceDf).get

    val sourcePlanProperties = SparkPlan.Properties(
      Seq(relation),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(Seq(
          Signature("com.microsoft.hyperspace.goldstandard.MockSignatureProvider", tableName)))))

    val entry = IndexLogEntry(
      config.indexName,
      CoveringIndex(
        CoveringIndex.Properties(
          CoveringIndex.Properties
            .Columns(config.indexedColumns, config.includedColumns),
          IndexLogEntry.schemaString(indexSchema),
          200,
          Map("hasParquetAsSourceFormat" -> "true"))),
      Content(Directory.fromDirectory(new Path(path), new FileIdTracker)),
      Source(SparkPlan(sourcePlanProperties)),
      Map())
    entry.state = Constants.States.ACTIVE
    entry
  }
}
