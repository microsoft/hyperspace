import unittest
import tempfile
import shutil
import os
import sys

from pyspark.sql import SQLContext

from hyperspace import *
from hyperspace.testing.utils import HyperspaceTestCase


class HyperspaceIndexUtilizationTests(HyperspaceTestCase):
    def setUp(self):
        super(HyperspaceIndexUtilizationTests, self).setUp()
        self.temp_path = tempfile.mkdtemp()
        self.temp_index_path = tempfile.mkdtemp()
        self.spark.conf.set("spark.hyperspace.index.creation.path", self.temp_index_path)
        self.spark.conf.set("spark.hyperspace.system.path", self.temp_index_path)
        data = [('Alice', 25, 'Seattle', 'Dept1'), ('Bob', 27, 'Bellevue', 'Dept2')]
        df = self.spark.createDataFrame(data, ['name', 'age', 'city', 'department'])
        self.data_file = os.path.join(self.temp_path, "tempFile.parquet")
        df.write.parquet(self.data_file)
        self.df = self.spark.read.parquet(self.data_file)
        self.hyperspace = Hyperspace(self.spark)

    def tearDown(self):
        self.spark.stop()
        shutil.rmtree(self.temp_path)
        shutil.rmtree(self.temp_index_path)
        super(HyperspaceIndexUtilizationTests, self).tearDown()

    def test_hyperspace_enable_disable(self):
        self.assertFalse(Hyperspace.isEnabled(self.spark), 
                         "Hyperspace must be disabled by default.")
        Hyperspace.enable(self.spark)
        self.assertTrue(Hyperspace.isEnabled(self.spark), 
                        "Hyperspace must be enabled after Hyperspace enable.")
        Hyperspace.disable(self.spark)
        self.assertFalse(Hyperspace.isEnabled(self.spark), 
                         "Hyperspace must be disabled after Hyperspace disable.")

    def test_hyperspace_explain(self):
        idx_config = IndexConfig('idx1', ['age'], ['name', 'department'])
        self.hyperspace.createIndex(self.df, idx_config)
        self.df.createOrReplaceTempView("employees")
        filter_query = self.spark.sql(""" SELECT age, name, department FROM employees 
                                          WHERE employees.age > 26 """)

        def verify_result(input):
            planAnalyzer = self.spark._jvm.com.microsoft.hyperspace.index.plananalysis.PlanAnalyzer
            jvmExplainString = planAnalyzer.explainString(filter_query._jdf, 
                                                          self.spark._jsparkSession,
                                                          self.hyperspace.indexes()._jdf, False)
            self.assertTrue(input == jvmExplainString)
        self.hyperspace.explain(filter_query, False, verify_result)

hyperspace_test = unittest.TestLoader().loadTestsFromTestCase(HyperspaceIndexUtilizationTests)
result = unittest.TextTestRunner(verbosity=3).run(hyperspace_test)
sys.exit(not result.wasSuccessful())
