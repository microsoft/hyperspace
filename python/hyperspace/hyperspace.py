import sys

from pyspark.sql import SparkSession, DataFrame
from py4j.java_gateway import java_import

from .indexconfig import *


class Hyperspace:
    def __init__(self, spark):
        """
        Initializes Hyperspace object.
        :param spark: sparkSession object
        :return: Hyperspace object

        >>> hyperspace = Hyperspace(spark)
        """
        self.spark = spark
        self.jvm = spark._jvm
        self.hyperspace = self.jvm.com.microsoft.hyperspace.Hyperspace(spark._jsparkSession)

    def _getJavaIndexConfig(self, index_config):
        """
        Constructs IndexConfig Java object from python wrapper IndexConfig object.
        :param index_config: IndexConfig java object
        :return: IndexConfig python object

        >>> _getJavaIndexConfig(idx_config)
        """
        indexed_columns = self._getScalaSeqFromList(index_config.indexedColumns)
        included_columns = self._getScalaSeqFromList(index_config.includedColumns)
        _jindexConfig = self.jvm.com.microsoft.hyperspace.index.IndexConfig(
            self.jvm.java.lang.String(index_config.indexName), indexed_columns, included_columns)
        return _jindexConfig

    def _getScalaSeqFromList(self, list):
        """
        Constructs scala sequence from Java's List object.
        :param list: List object in Java
        :return: Seq object in scala

        >>> _getScalaSeqFromList(list)
        """
        java_import(self.jvm, "scala.collection.JavaConversions._")
        java_import(self.jvm, "scala.collection.Seq")
        java_import(self.jvm, 'java.util.*')
        result_array_list = self.jvm.ArrayList(len(list))
        for element in list:
            result_array_list.add(self.jvm.String(element))
        return self.jvm.scala.collection.JavaConverters.asScalaIteratorConverter(
            result_array_list.iterator()).asScala().toSeq()

    def indexes(self):
        """
        Gets available indexes.
        :return: dataFrame object containing list of indexes.

        >>> hyperspace = Hyperspace(spark)
        >>> hyperspace.indexes()
        """
        return DataFrame(self.hyperspace.indexes(), self.spark._wrapped)

    def createIndex(self, dataFrame, indexConfig):
        """
        Creates index on the given dataframe using the given indexConfig.
        :param dataFrame: dataFrame
        :param indexConfig: indexConfig

        >>> hyperspace = Hyperspace(spark)
        >>> idxConfig = IndexConfig("indexName", ["c1"], ["c2","c3"])
        >>> df = spark.read.parquet("./sample.parquet").toDF("c1", "c2", "c3")
        >>> hyperspace.createIndex(df, indexConfig)
        """
        self.hyperspace.createIndex(dataFrame._jdf, self._getJavaIndexConfig(indexConfig))

    def deleteIndex(self, indexName):
        """
        Soft deletes given index.
        :param indexName: index name

        >>> hyperspace = Hyperspace(spark)
        >>> hyperspace.deleteIndex("indexname")
        """
        self.hyperspace.deleteIndex(indexName)

    def restoreIndex(self, indexName):
        """
        Restores index with given index name.
        :param indexName: index name

        >>> hyperspace = Hyperspace(spark)
        >>> hyperspace.restoreIndex("indexname")
        """
        self.hyperspace.restoreIndex(indexName)

    def vacuumIndex(self, indexName):
        """
        Vacuums index with given index name.
        :param indexName: index name

        >>> hyperspace = Hyperspace(spark)
        >>> hyperspace.vacuumIndex("indexname")
        """
        self.hyperspace.vacuumIndex(indexName)

    def refreshIndex(self, indexName, mode='full'):
        """
        Update indexes for the latest version of the data.
        :param indexName: index name
        :param mode: refresh mode 'incremental' or 'full'

        >>> hyperspace = Hyperspace(spark)
        >>> hyperspace.refreshIndex("indexname", "incremental/full")
        """
        self.hyperspace.refreshIndex(indexName, mode)

    def optimizeIndex(self, indexName, mode='quick'):
        """
        Optimize index by changing the underlying index data layout (e.g., compaction).
        :param indexName: index name
        :param mode: optimize mode 'quick' or 'full'

        >>> hyperspace = Hyperspace(spark)
        >>> hyperspace.optimizeIndex("indexname", "quick/full")
        """
        self.hyperspace.optimizeIndex(indexName, mode)

    def cancel(self, indexName):
        """
        Cancel api to bring back index from an inconsistent state to the last known stable state.
        :param indexName: index name

        >>> hyperspace = Hyperspace(spark)
        >>> hyperspace.cancel("indexname")
        """
        self.hyperspace.cancel(indexName)

    def explain(self, df, verbose=False, redirectFunc=lambda x: sys.stdout.write(x)):
        """
        Explains how indexes will be applied to the given dataframe.
        :param df: dataFrame
        :param redirectFunc: optional function to redirect output of explain

        >>> hyperspace = Hyperspace(spark)
        >>> df = spark.read.parquet("./sample.parquet").toDF("c1", "c2", "c3")
        >>> hyperspace.explain(df)
        """
        analyzer = self.jvm.com.microsoft.hyperspace.index.plananalysis.PlanAnalyzer
        result_string = analyzer.explainString(df._jdf, self.spark._jsparkSession,
                                               self.hyperspace.indexes(), verbose)
        redirectFunc(result_string)

    @staticmethod
    def enable(spark):
        """
        Enables Hyperspace index usage on given spark session.
        :param spark: sparkSession

        >>> Hyperspace.enable(spark)
        """
        spark._jvm.com.microsoft.hyperspace.util.PythonUtils.enableHyperspace(spark._jsparkSession)
        return spark

    @staticmethod
    def disable(spark):
        """
        Disables Hyperspace index usage on given spark session.
        :param spark: sparkSession

        >>> Hyperspace.disable(spark)
        """
        spark._jvm.com.microsoft.hyperspace.util.PythonUtils.disableHyperspace(spark._jsparkSession)
        return spark

    @staticmethod
    def isEnabled(spark):
        """
        Checks if Hyperspace is enabled or not.
        :param spark: sparkSession

        >>> Hyperspace.isEnabled(spark)
        """
        return spark._jvm.com.microsoft.hyperspace.util.PythonUtils. \
            isHyperspaceEnabled(spark._jsparkSession)
