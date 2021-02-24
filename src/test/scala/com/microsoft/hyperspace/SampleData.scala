/*
 * Copyright (2020) The Hyperspace Project Authors.
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

import org.apache.spark.sql.SparkSession

/**
 * Sample data for testing.
 */
object SampleData {
  val testData = Seq(
    ("2017-09-03", "810a20a2baa24ff3ad493bfbf064569a", "donde", 2, 1000),
    ("2017-09-03", "fd093f8a05604515957083e70cb3dceb", "facebook", 1, 3000),
    ("2017-09-03", "af3ed6a197a8447cba8bc8ea21fad208", "facebook", 1, 3000),
    ("2017-09-03", "975134eca06c4711a0406d0464cbe7d6", "facebook", 1, 4000),
    ("2018-09-03", "e90a6028e15b4f4593eef557daf5166d", "ibraco", 2, 3000),
    ("2018-09-03", "576ed96b0d5340aa98a47de15c9f87ce", "facebook", 2, 3000),
    ("2018-09-03", "50d690516ca641438166049a6303650c", "ibraco", 2, 1000),
    ("2019-10-03", "380786e6495d4cd8a5dd4cc8d3d12917", "facebook", 2, 3000),
    ("2019-10-03", "ff60e4838b92421eafc3e6ee59a9e9f1", "miperro", 2, 2000),
    ("2019-10-03", "187696fe0a6a40cc9516bc6e47c70bc1", "facebook", 4, 3000))

  val testDataMarvel = Seq(
    (1941, "Captain America", "Shield", "Avengers", "Marvel"),
    (1966, "Black Panther", "Claws", "Avengers", "Marvel"),
    (1973, "Blade", "Sword", "Vampire", "Marvel"),
    (1974, "Wolverine", "Claws", "XMen", "Marvel"),
    (1964, "Scarlet Witch", "Telepathy", "Avengers", "Marvel"),
    (1968, "Vision", "Mind Stone", "Avengers", "Marvel"),
    (1972, "Ghost Rider", "Soul", "SHIELD", "Marvel"),
    (1962, "Hulk", "Gamma Radiation Strength", "Avengers", "Marvel"),
    (1963, "Spider Man", "Spider Web", "Avengers", "Marvel"),
    (1948, "Loki", "Magic Sword", "Avengers", "Marvel"),
    (1966, "Silver Surfer", "Board", "Fantastic Four", "Marvel"),
    (1968, "Doctor Strange", "Eye of Agamotto", "Avengers", "Marvel"),
    (1963, "Cyclops", "Optic eyes", "XMen", "Marvel"),
    (1963, "Beast", "Strength", "XMen", "Marvel"),
    (1963, "Iceman", "Ice", "XMen", "Marvel"),
    (1963, "Jean Grey", "Telepathy", "XMen", "Marvel"),
    (1963, "Nick Fury", "One eye", "SHIELD", "Marvel"),
    (1963, "Deadpool", "Katanas", "XMen", "Marvel"),
    (1963, "Iron Man", "Suit", "Avengers", "Marvel"),
    (1963, "ProfessorX", "Telepathy", "XMen", "Marvel"),
    (1973, "Thanos", "Infinity Gauntlet", "Avengers", "Marvel"),
    (1962, "Thor", "Hammer", "Avengers", "Marvel"),
    (1962, "Ant Man", "Pym Particles", "Avengers", "Marvel"),
    (1963, "Magneto", "Magnetic Forces", "XMen", "Marvel"),
    (1976, "Star Lord", "Half Human Half Celestial", "Guardians Of Galaxy", "Marvel"),
  )

  val testDataDC = Seq(
    (1941, "Aquaman", "Water", "Justice League", "DC"),
    (1941, "Wonder Woman", "Lasso", "Justice League", "DC"),
    (1940, "Shazam", "Magic", "Marvel Family", "DC"),
    (1938, "Superman", "Superhuman Strength", "Justice League", "DC"),
    (1956, "Flash", "Speed", "Justice League", "DC"),
    (1959, "Green Lantern", "Ring", "Justice League", "DC"),
    (1939, "Batman", "None", "Justice League", "DC"),
    (1940, "Joker", "Chemist Engineer", "Injustice League", "DC"),
    (1992, "Harley Quinn", "Gymnast", "Suicide Squad", "DC"),
    (1950, "Deadshot", "Marksman", "Suicide Squad", "DC"))

  def save(
      spark: SparkSession,
      path: String,
      columns: Seq[String],
      partitionColumns: Option[Seq[String]] = None): Unit = {
    import spark.implicits._
    val df = testData.toDF(columns: _*)
    partitionColumns match {
      case Some(pcs) =>
        df.write.partitionBy(pcs: _*).parquet(path)
      case None =>
        df.write.parquet(path)
    }
  }

  def saveComics(
      spark: SparkSession,
      data: Seq[(Int, String, String, String, String)],
      path: String,
      columns: Seq[String],
      partitionColumns: Option[Seq[String]] = None): Unit = {
    import spark.implicits._
    val df = data.toDF(columns: _*)
    partitionColumns match {
      case Some(pcs) =>
        df.write.partitionBy(pcs: _*).parquet(path)
      case None =>
        df.write.parquet(path)
    }
  }
}
