import unittest
import tempfile
import shutil
import os
import sys

from pyspark.sql import SparkSession

from hyperspace import *
from hyperspace.testing.utils import HyperspaceTestCase


class HyperspaceIndexManagementTests(HyperspaceTestCase):
    def setUp(self):
        super(HyperspaceIndexManagementTests, self).setUp()
        self.temp_path = tempfile.mkdtemp()
        self.temp_index_path = tempfile.mkdtemp()
        self.spark.conf.set("spark.hyperspace.index.creation.path", self.temp_index_path)
        self.spark.conf.set("spark.hyperspace.system.path", self.temp_index_path)
        data = [('Alice', 25, 'Seattle'), ('Bob', 27, 'Bellevue')]
        df = self.spark.createDataFrame(data, ['name', 'age', 'city'])
        self.data_file = os.path.join(self.temp_path, "tempFile.parquet")
        df.write.parquet(self.data_file)
        self.df = self.spark.read.parquet(self.data_file)
        self.hyperspace = Hyperspace(self.spark)

    def tearDown(self):
        shutil.rmtree(self.temp_path)
        shutil.rmtree(self.temp_index_path)
        super(HyperspaceIndexManagementTests, self).tearDown()

    def test_index_create(self):
        idx_config = IndexConfig('idx1', ['name'], ['age'])
        self.hyperspace.createIndex(self.df, idx_config)
        self.assertEqual(self.hyperspace.indexes().filter("""name = "idx1" """).count(), 1)

    def test_index_delete(self):
        idx_config = IndexConfig('idx2', ['name'], ['age'])
        self.hyperspace.createIndex(self.df, idx_config)
        self.assertEqual(self.hyperspace.indexes().filter(
            """name = "idx2" and state = "ACTIVE" """).count(), 1)
        self.assertEqual(self.hyperspace.indexes().filter(
            """name = "idx2" and state = "DELETED" """).count(), 0)
        self.hyperspace.deleteIndex("idx2")
        self.assertEqual(self.hyperspace.indexes().filter(
            """name = "idx2" and state = "DELETED" """).count(), 1)
        self.assertEqual(self.hyperspace.indexes().filter(
            """name = "idx2" and state = "ACTIVE" """).count(), 0)

    def test_index_restore(self):
        idx_config = IndexConfig('idx3', ['name'], ['age'])
        self.hyperspace.createIndex(self.df, idx_config)
        self.hyperspace.deleteIndex("idx3")
        self.assertEqual(self.hyperspace.indexes().filter(
            """name = "idx3" and state = "DELETED" """).count(), 1)
        self.hyperspace.restoreIndex("idx3")
        self.assertEqual(self.hyperspace.indexes().filter(
            """name = "idx3" and state = "ACTIVE" """).count(), 1)
        self.assertEqual(self.hyperspace.indexes().filter(
            """name = "idx3" and state = "DELETED" """).count(), 0)

    def test_index_vacuum(self):
        idx_config = IndexConfig('idx4', ['name'], ['age'])
        self.hyperspace.createIndex(self.df, idx_config)
        self.hyperspace.deleteIndex("idx4")
        self.assertEqual(self.hyperspace.indexes().filter(
            """name = "idx4" and state = "DELETED" """).count(), 1)
        self.hyperspace.vacuumIndex("idx4")
        self.assertEqual(self.hyperspace.indexes().filter("""name = "idx4" """).count(), 0)

        # vacuuming of active index leaves the index as active
        idx_config = IndexConfig('idx5', ['name'], ['age'])
        self.hyperspace.createIndex(self.df, idx_config)
        self.assertEqual(self.hyperspace.indexes().filter(
            """name = "idx5" and state = "ACTIVE" """).count(), 1)

    def test_index_refresh_incremental(self):
        idx_config = IndexConfig('idx1', ['name'], ['age'])
        self.hyperspace.createIndex(self.df, idx_config)
        # Test the inter-op works fine for refreshIndex.
        self.hyperspace.refreshIndex('idx1')
        self.hyperspace.refreshIndex('idx1', 'incremental')

    def test_index_refresh_full(self):
        idx_config = IndexConfig('idx1', ['name'], ['age'])
        self.hyperspace.createIndex(self.df, idx_config)
        # Test the inter-op works fine for optimizeIndex.
        self.hyperspace.optimizeIndex('idx1')
        self.hyperspace.optimizeIndex('idx1', 'full')

    def test_index_metadata(self):
        idx_config = IndexConfig('idx1', ['name'], ['age'])
        self.hyperspace.createIndex(self.df, idx_config)
        # Test the inter-op works fine for "index" API.
        df = self.hyperspace.index('idx1')
        df.show()


hyperspace_test = unittest.TestLoader().loadTestsFromTestCase(HyperspaceIndexManagementTests)
result = unittest.TextTestRunner(verbosity=3).run(hyperspace_test)
sys.exit(not result.wasSuccessful())
