import unittest
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


class HyperspaceTestCase(unittest.TestCase):
    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.spark = SparkSession.builder \
            .master("local[4]") \
            .config("spark.ui.enabled", "false") \
            .config("spark.hyperspace.index.numBuckets", "5") \
            .appName(class_name) \
            .getOrCreate()

    def tearDown(self):
        self.spark.stop()
        sys.path = self._old_sys_path
