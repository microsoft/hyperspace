---
layout: splash
permalink: /
hidden: true
header:
  overlay_color: "#5e616c"
  overlay_image: /assets/images/hyperspace-banner.jpg
  actions:
    - label: "<i class='fas fa-rocket'></i> Get started now"
      url: "/docs/ug-quick-start-guide/"
excerpt: >
  A simple system that allows users to build, maintain and leverage indexes automagically for query/workload acceleration.<br />
  <small><a href="https://github.com/microsoft/hyperspace/releases/tag/v0.4.0">Latest release v0.4.0</a></small>
features:
  - image_path: /assets/images/hyperspace-overview-simple.png
    alt: "simple"
    title: "Simple API"
    excerpt: "With simple API such as create, refresh, delete, restore, vacuum and cancel, Hyperspace helps you get started easily!"
  - image_path: /assets/images/hyperspace-overview-multi.png
    alt: "multi-language support"
    title: "Multi-language support"
    excerpt: "Don't know Scala? Don't worry! Hyperspace supports Scala, Python and .NET allowing you to be productive right-away."
  - image_path: /assets/images/hyperspace-overview-works.png
    alt: "works out of box"
    title: "Just works!"
    excerpt: "Hyperspace works out-of-box with open source Apache Spark™ v2.4 and does not depend on any service."
---

Hyperspace is an early-phase indexing subsystem for Apache Spark™ that
introduces the ability for users to build indexes on their data, maintain them
through a multi-user concurrency mode, and leverage them automatically -
without any change to their application code - for query/workload
acceleration.

Users utilize a set of simple APIs exposed by Hyperspace to make use of its
powerful capabilities.

{% include feature_row_small id="features" %}

# Hyperspace Usage API in Apache Spark™

```scala
import org.apache.spark.sql._
import com.microsoft.hyperspace._
import com.microsoft.hyperspace.index._

object HyperspaceSampleApp extends App {
  val spark = SparkSession.builder().appName("main").master("local").getOrCreate()

  import spark.implicits._

  // Create sample data and read it back as a DataFrame
  Seq((1, "name1"), (2, "name2")).toDF("id", "name").write.mode("overwrite").parquet("table")
  val df = spark.read.parquet("table")

  // Create Hyperspace object
  val hs = new Hyperspace(spark)

  // Create indexes
  hs.createIndex(df, IndexConfig("index1", indexedColumns = Seq("id"), includedColumns = Seq("name")))
  hs.createIndex(df, IndexConfig("index2", indexedColumns = Seq("name")))

  // Display available indexes
  hs.indexes.show

  // Simple filter query
  val query = df.filter(df("id") === 1).select("name")

  // Check if any indexes will be utilized
  hs.explain(query, verbose = true)

  // Now utilize the indexes
  spark.enableHyperspace
  query.show

  // The followings are the index management APIs.
  hs.refreshIndex("index1")
  hs.deleteIndex("index1")
  hs.restoreIndex("index1")
  hs.deleteIndex("index2")
  hs.vacuumIndex("index2")

  // Clean up
  spark.stop
}
```


