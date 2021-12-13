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

package com.microsoft.hyperspace.util

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.functions.{col, input_file_name, max, min}
import org.apache.spark.sql.types.{StructField, StructType}

import com.microsoft.hyperspace.HyperspaceException

case class MinMaxAnalysisResult(
    colName: String,
    maxNumFilesForValueRanges: Seq[Int],
    minVal: String,
    maxVal: String,
    totalNumFiles: Int,
    totalBytes: Long,
    maxBytesToLookup: Long) {
  lazy val maxNumFilesToLookup: Int = {
    if (maxNumFilesForValueRanges.nonEmpty) {
      maxNumFilesForValueRanges.max
    } else {
      -1
    }
  }

  lazy val avgNumFilesToLookup: Double = {
    maxNumFilesForValueRanges.sum / maxNumFilesForValueRanges.count(_ > 0).toDouble
  }

  def appendSummary(newline: String, stringBuilder: StringBuilder): Unit = {
    if (minVal != null) {
      Seq(
        s"min($colName): $minVal",
        s"max($colName): $maxVal",
        s"Total num of files: $totalNumFiles",
        s"Total byte size of files: $totalBytes",
        s"Max. num of files for a point lookup: $maxNumFilesToLookup (" +
          "%.2f".format((maxNumFilesToLookup / totalNumFiles.toDouble) * 100) +
          "%)",
        s"Estimated average num of files for a point lookup: " +
          "%.2f".format(avgNumFilesToLookup) + " (" +
          "%.2f".format((avgNumFilesToLookup / totalNumFiles) * 100) + "%)",
        s"Max. bytes to read for a point lookup: $maxBytesToLookup (" +
          "%.2f".format((maxBytesToLookup / totalBytes.toDouble) * 100) + "%)")
        .foreach { line =>
          stringBuilder.append(line)
          stringBuilder.append(newline)
        }
    } else {
      Seq(
        s"min($colName): $minVal",
        s"max($colName): $maxVal",
        s"Total num of files: $totalNumFiles",
        s"Total byte size of files: $totalBytes")
        .foreach { line =>
          stringBuilder.append(line)
          stringBuilder.append(newline)
        }
    }
  }
}

object MinMaxAnalysisResult {
  def allNullValueResult(colName: String, numFiles: Int, numBytes: Long): MinMaxAnalysisResult = {
    MinMaxAnalysisResult(colName, Nil, null, null, numFiles, numBytes, 0)
  }
}

trait MinMaxResultWriter {
  val stringBuilder = new StringBuilder
  val newline: String
  val distFigureWidth: Int
  def appendEmptyLine(): Unit = {
    stringBuilder.append(newline)
  }
  def appendLine(line: String): Unit = {
    stringBuilder.append(line)
    stringBuilder.append(newline)
  }
  def appendResult(result: MinMaxAnalysisResult)
  def appendComparisonResult(beforeRes: MinMaxAnalysisResult, afterRes: MinMaxAnalysisResult)
}

case class TextResultWriter() extends MinMaxResultWriter {
  override val newline: String = System.lineSeparator()
  override val distFigureWidth: Int = 50
  override def appendResult(res: MinMaxAnalysisResult): Unit = {
    if (res.minVal == null) {
      res.appendSummary(newline, stringBuilder)
    } else {
      val summaryStringBuilder = new StringBuilder
      res.appendSummary(newline, summaryStringBuilder)
      stringBuilder.append(textResultString(res, summaryStringBuilder.mkString))
    }
  }

  override def appendComparisonResult(
      beforeRes: MinMaxAnalysisResult,
      afterRes: MinMaxAnalysisResult): Unit = {
    if (afterRes.minVal == null) {
      afterRes.appendSummary(newline, stringBuilder)
    } else {
      val beforeSummaryStringBuilder = new StringBuilder
      beforeSummaryStringBuilder.append(
        s"Analysis result on column ${beforeRes.colName} before Z-ORDER OPTIMIZE")
      beforeSummaryStringBuilder.append(newline)
      beforeRes.appendSummary(newline, beforeSummaryStringBuilder)
      val beforeSummaryString = beforeSummaryStringBuilder.mkString

      val afterSummaryStringBuilder = new StringBuilder
      afterSummaryStringBuilder.append(
        s"Analysis result on column ${afterRes.colName} after Z-ORDER OPTIMIZE")
      afterSummaryStringBuilder.append(newline)
      afterRes.appendSummary(newline, afterSummaryStringBuilder)
      val afterSummaryString = afterSummaryStringBuilder.mkString

      val beforeResString = textResultString(beforeRes, beforeSummaryString)
      val afterResString = textResultString(afterRes, afterSummaryString)
      appendLine(mergeResultString(beforeResString, afterResString))
      appendEmptyLine()
    }
  }

  private def mergeResultString(beforeResString: String, afterResString: String): String = {
    val beforeLines = beforeResString.split(newline)
    val afterLines = afterResString.split(newline)

    val beforeLinesMaxWidth = beforeLines.map(_.length).max
    val betweenSpaces = 20
    beforeLines
      .zip(afterLines)
      .map {
        case (b, a) =>
          val betweenStr = if (b.contains("50%")) {
            val arrow = "------->>>"
            val arrowStr = {
              val spaces = betweenSpaces - arrow.length
              val left = " " * ((spaces + 1) / 2)
              val right = " " * (spaces / 2)
              s"$left$arrow$right"
            }
            " " * (beforeLinesMaxWidth - b.length) + arrowStr
          } else {
            " " * (beforeLinesMaxWidth - b.length + betweenSpaces)
          }
          s"$b$betweenStr$a"
      }
      .mkString(newline)
  }

  private def textResultString(res: MinMaxAnalysisResult, summaryString: String): String = {
    val builder = new StringBuilder
    // Draw distribution figure in text.
    builder.append(s"Min/Max analysis on ${res.colName}")
    builder.append(newline)
    builder.append(newline)
    val leftMargin = 5
    val leftMarginSpaceStr = " " * 5
    builder.append(" " * leftMargin)
    val title = "< Number of files (%) >"
    builder.append(" " * ((distFigureWidth - title.length) / 2))
    builder.append(title)
    builder.append(newline)
    builder.append(leftMarginSpaceStr)
    builder.append("+")
    builder.append("-" * distFigureWidth)
    builder.append("+")
    builder.append(newline)
    val distFigureHeight = 20
    val histUnit = res.totalNumFiles / distFigureHeight.toDouble
    val tickNumbers = Seq(0.0, 0.25, 0.50, 0.75, 1.0).map { n =>
      (distFigureHeight * n).min(distFigureHeight - 1) -> s"${(n * 100).toInt}%"
    }.toMap
    (0 until distFigureHeight).reverse.foreach { u =>
      val tick = tickNumbers.getOrElse(u, "")
      builder.append(" " * (leftMargin - tick.length - 1))
      builder.append(tick)
      builder.append(" |")

      builder.append(res.maxNumFilesForValueRanges.map { n =>
        val h = (n / histUnit).ceil.toInt
        if (h > u) {
          '*'
        } else {
          ' '
        }
      }.mkString)
      builder.append("|")
      builder.append(newline)
    }
    builder.append(leftMarginSpaceStr)
    builder.append("+")
    builder.append("-" * distFigureWidth)
    builder.append("+")
    builder.append(newline)

    val xAxisLeft = "Min <----- "
    val xAxisRight = " -----> Max"
    val valueStr = " value"
    val colNameLen = distFigureWidth + 2 - xAxisLeft.length - xAxisRight.length - valueStr.length
    val colNamePart = {
      val colNameClipped = if (res.colName.length > colNameLen) {
        res.colName.slice(0, colNameLen)
      } else {
        res.colName
      }
      val spaces = colNameLen - colNameClipped.length
      val left = " " * ((spaces + 1) / 2)
      val right = " " * (spaces / 2)
      s"$left${res.colName}$valueStr$right"
    }
    builder.append(leftMarginSpaceStr)
    builder.append(s"$xAxisLeft$colNamePart$xAxisRight")
    builder.append(newline)
    builder.append(newline)
    builder.append(summaryString)
    builder.append(newline)
    builder.append(newline)
    builder.mkString
  }
}

case class HtmlResultWriter() extends MinMaxResultWriter {
  override val distFigureWidth: Int = 30
  override val newline: String = "<br>"

  private val blueLineArea = ("rgb(0, 102, 204)", "rgb(204, 229, 255)")
  private val redLineArea = ("rgb(102, 0, 0)", "rgb(255, 204, 204)")
  private val yellowLineArea = ("rgb(255,140,0)", "rgb(255, 255, 204)")

  override def appendResult(res: MinMaxAnalysisResult): Unit = {
    if (res.minVal == null) {
      res.appendSummary(newline, stringBuilder)
    } else {
      val summaryStringBuilder = new StringBuilder
      summaryStringBuilder.append(s"Analysis result on column ${res.colName}")
      summaryStringBuilder.append(newline)
      val summaryString = res.appendSummary(newline, summaryStringBuilder)
      appendLine(
        d3HtmlString(
          res.colName,
          res.maxNumFilesForValueRanges,
          res.totalNumFiles,
          summaryStringBuilder.mkString))
      appendEmptyLine()
    }
  }

  override def appendComparisonResult(
      beforeRes: MinMaxAnalysisResult,
      afterRes: MinMaxAnalysisResult): Unit = {
    if (afterRes.minVal == null) {
      afterRes.appendSummary(newline, stringBuilder)
    } else {
      val beforeSummaryStringBuilder = new StringBuilder
      beforeSummaryStringBuilder.append(
        s"Analysis result on column ${beforeRes.colName} before ZORDER OPTIMIZE")
      beforeSummaryStringBuilder.append(newline)
      beforeRes.appendSummary(newline, beforeSummaryStringBuilder)
      val beforeSummaryString = beforeSummaryStringBuilder.mkString

      val afterSummaryStringBuilder = new StringBuilder
      afterSummaryStringBuilder.append(
        s"Analysis result on column ${afterRes.colName} after ZORDER OPTIMIZE")
      afterSummaryStringBuilder.append(newline)
      afterRes.appendSummary(newline, afterSummaryStringBuilder)
      val afterSummaryString = afterSummaryStringBuilder.mkString

      // Compare the result for coloring graph.
      val avgDiff =
        (afterRes.avgNumFilesToLookup / afterRes.totalNumFiles) -
          (beforeRes.avgNumFilesToLookup / beforeRes.totalNumFiles)
      val (beforeColor, afterColor) = if (avgDiff.abs <= 0.05) {
        // Diff is less than 5%
        (yellowLineArea, yellowLineArea)
      } else if (avgDiff < 0) {
        // After is better
        (redLineArea, blueLineArea)
      } else {
        // Before is better
        (blueLineArea, redLineArea)
      }

      stringBuilder.append(
        d3HtmlStringForComparison(
          beforeRes.colName,
          beforeRes.maxNumFilesForValueRanges,
          beforeRes.totalNumFiles,
          beforeSummaryString,
          beforeColor,
          afterRes.maxNumFilesForValueRanges,
          afterRes.totalNumFiles,
          afterSummaryString,
          afterColor))
      appendEmptyLine()
      appendEmptyLine()
    }
  }

  private def d3HtmlGraphScript(
      colName: String,
      svgId: String,
      graphTitle: String,
      values: Seq[Int],
      yMax: Int,
      lineAreaColor: (String, String)): String = {
    val valueStr = values.mkString(",")
    s"""<script>
      var margin = {"top": 40, "right": 30, "bottom": 25, "left": 50 }
      var width = 500;
      var height = 500;
      var lineWidth = (500 - margin.left - margin.right) / ${values.length - 1}

      var data = [$valueStr];

      var xMax = ${values.length} * lineWidth
      var xScale = d3.scaleLinear()
        .domain([0, xMax])
        .range([margin.left, width - margin.right]);

      var yMax = $yMax;
      var yScale = d3.scaleLinear()
        .domain([0, yMax])
        .range([height - margin.bottom, margin.top]);

      var svg = d3.select('#svg$svgId');

      svg.append("path")
         .datum(data)
         .attr("fill", "${lineAreaColor._2}")
         .attr("d", d3.area()
            .x(function(d, i) { return i * lineWidth + margin.left; })
            .y0(height - margin.bottom)
            .y1(function(d, i) { return yScale(d); })
          )

      svg.append("path")
        .datum(data)
        .attr("fill", "none")
        .attr("stroke", "${lineAreaColor._1}")
        .attr("stroke-width", 1.5)
        .attr("d", d3.line()
          .x(function(d, i) { return i * lineWidth + margin.left; })
          .y(function(d, i) { return yScale(d); })
          )

      var xAxis = d3.axisBottom()
        .scale(xScale)
        .tickFormat((d, i) => ['Min', 'Max'][i])
        .tickValues([0, xMax]);
      var yAxis = d3.axisLeft()
        .scale(yScale)
        .tickFormat((d, i) => ['0%', '25%', '50%', '75%', '100%'][i])
        .tickValues([0, ${yMax / 4.0}, ${yMax / 2.0}, ${yMax * 0.75}, $yMax]);

      svg.append('g')
        .attr('transform', 'translate(' + [0, height - margin.bottom] + ')')
        .call(xAxis);
      svg.append('g')
        .attr('transform', 'translate(' + [margin.left, 0] + ')')
        .call(yAxis);

      svg.append("text")
        .attr("text-anchor", "middle")
        .attr("transform", "translate(" + 12 + "," + (height / 2) + ")rotate(-90)")
        .style("font-size", "15px")
        .text("Number of files (%)");

      svg.append("text")
        .attr("text-anchor", "middle")
        .attr("transform", "translate(" + (width / 2) + "," + (height - 4) + ")")
        .style("font-size", "15px")
        .text("$colName value");

      svg.append("text")
        .attr("text-anchor", "middle")
        .attr("transform", "translate(" + (width / 2) + "," + 20 + ")")
        .style("font-size", "17px")
        .text("$graphTitle");
    </script>"""
  }

  private def d3HtmlTextSpans(lines: String): String = {
    lines
      .split(newline)
      .map { line =>
        if (line.nonEmpty) {
          s"""<tspan x=0 dy="1.2em">$line</tspan>"""
        } else {
          """<tspan x=0 dy="1.2em"> </tspan>"""
        }
      }
      .mkString("")
  }

  private def d3HtmlString(
      colName: String,
      values: Seq[Int],
      yMax: Int,
      summaryTextHtml: String): String = {

    val textSpans = d3HtmlTextSpans(summaryTextHtml)
    val title = s"Min/Max analysis on $colName"
    val scriptString = d3HtmlGraphScript(colName, colName, title, values, yMax, blueLineArea)

    s"""
    <!DOCTYPE html>
    <meta charset="utf-8">
    <script src="https://d3js.org/d3.v4.js"></script>
    $scriptString
    <body>
    <svg width=500 height=700>
      <g transform="translate(0, 0)">
        <svg width=500 height=500 id="svg$colName"></svg>
      </g>
      <g transform="translate(0, 520)">
        <svg width=500 height=200>
          <text x=0 y=0>
            $textSpans
          </text>
       </svg>
    </g></svg><br></body>
    """
  }

  private def d3HtmlStringForComparison(
      colName: String,
      beforeValues: Seq[Int],
      beforeYMax: Int,
      beforeSummary: String,
      beforeColor: (String, String),
      afterValues: Seq[Int],
      afterYMax: Int,
      afterSummary: String,
      afterColor: (String, String)): String = {
    val beforeTextSpans = d3HtmlTextSpans(beforeSummary)
    val beforeTitle = s"Min/max analysis on $colName"
    val beforeScriptString =
      d3HtmlGraphScript(colName, s"B$colName", beforeTitle, beforeValues, beforeYMax, beforeColor)
    val afterTextSpans = d3HtmlTextSpans(afterSummary)
    val afterTitle = s"Min/max analysis on $colName"
    val afterScriptString =
      d3HtmlGraphScript(colName, s"A$colName", afterTitle, afterValues, afterYMax, afterColor)

    s"""
    <!DOCTYPE html>
    <meta charset="utf-8">
    <script src="https://d3js.org/d3.v4.js"></script>
    $beforeScriptString
    $afterScriptString
    <body>
    <svg width=1200 height=700>
      <g transform="translate(0, 0)">
        <svg width=500 height=500 id="svgB$colName"></svg>
      </g>
      <g transform="translate(0, 520)">
        <svg width=500 height=200>
          <text x=0 y=0>
            $beforeTextSpans
          </text>
       </svg>
      </g>
      <g transform="translate(600, 0)">
        <svg width=500 height=500 id="svgA$colName"></svg>
      </g>
      <g transform="translate(600, 520)">
        <svg width=500 height=200>
          <text x=0 y=0>
            $afterTextSpans
          </text>
       </svg>
      </g>
      <g transform="translate(500, 250)">
        <svg witdh=70 height=50>
          <defs>
            <marker id="arrow" markerWidth="10" markerHeight="10" refX="0" refY="3"
              orient="auto" markerUnits="strokeWidth" viewBox="0 0 20 20">
              <path d="M0,0 L0,6 L9,3 z" fill="#000" />
            </marker>
          </defs>
        <line x1="0" y1="20" x2="50" y2="20" stroke="#000" stroke-width="3"
          marker-end="url(#arrow)" />
        </svg>
      </g>
    </svg><br></body>
    """
  }
}

object MinMaxResultWriter {
  def getWriter(format: String): MinMaxResultWriter = {
    format match {
      case "html" =>
        HtmlResultWriter()
      case "text" =>
        TextResultWriter()
    }
  }
}

trait MinMaxAnalysisHelper {
  private def extractStructField(
      spark: SparkSession,
      colName: String,
      schema: StructType): StructField = {
    val resolvedColNameParts = UnresolvedAttribute.parseAttributeName(colName).toList
    val resolver = spark.sessionState.conf.resolver

    def extractField(nameParts: Seq[String], struct: StructType): StructField = {
      nameParts match {
        case h :: tail =>
          val field = struct.find(f => resolver(f.name, h))
          field
            .map {
              case StructField(_, s: StructType, _, _) if tail.nonEmpty =>
                extractField(tail, s)
              case f =>
                // Return with full name for nested column.
                f.copy(name = colName)
            }
            .getOrElse {
              throw HyperspaceException(s"Cannot find column name: $colName")
            }
      }
    }
    extractField(resolvedColNameParts, schema)
  }

  def findNumericField(
      spark: SparkSession,
      schema: StructType,
      colNames: Seq[String]): (Seq[StructField], Seq[StructField]) = {
    val fields = colNames.map(col => extractStructField(spark, col, schema))
    fields.partition { t =>
      TypeUtils.checkForNumericExpr(t.dataType, "minMaxAnalysis").isSuccess
    }
  }

  def writeSkipMsgForNonNumeric(
      writer: MinMaxResultWriter,
      nonNumericCols: Seq[StructField]): Unit = {
    if (nonNumericCols.nonEmpty) {
      writer.appendLine(
        s"Skip non-numeric type columns: [${nonNumericCols.map(_.name).mkString(", ")}]")
      writer.appendEmptyLine()
    }
  }

  def normalizeDouble(d: Double): Double = {
    d match {
      case Double.PositiveInfinity =>
        Double.MaxValue
      case Double.NegativeInfinity =>
        Double.MinValue
      case Double.NaN =>
        Double.MaxValue
      case -0.0 =>
        0.0
      case _ =>
        d
    }
  }

  def collectMinMaxFromDf(
      df: DataFrame,
      cols: Seq[StructField]): Map[String, Map[String, Any]] = {
    // Collect min/max value of target columns for each input file.
    if (cols.isEmpty) {
      Map.empty
    } else {
      val aggs = cols.flatMap(
        c =>
          min(col(c.name)).as(s"min(${c.name})") ::
            max(col(c.name)).as(s"max(${c.name})") :: Nil)
      val stats = df
        .withColumn("fileName", input_file_name())
        .groupBy("fileName")
        .agg(aggs.head, aggs.tail: _*)
        .collect
        .toSeq

      stats.map { r =>
        val res = cols.flatMap { c =>
          val minIdx = r.fieldIndex(s"min(${c.name})")
          val maxIdx = r.fieldIndex(s"max(${c.name})")
          s"min(${c.name})" -> r.get(minIdx) :: s"max(${c.name})" -> r.get(maxIdx) :: Nil
        }
        val fileName = r.getAs[String]("fileName")
        fileName -> res.toMap
      }.toMap
    }
  }
}

trait MinMaxAnalyzer extends MinMaxAnalysisHelper {
  val writer: MinMaxResultWriter
  val sparkSession: SparkSession
  val schema: StructType
  val targetCols: Seq[String]

  def collectFileToSizeMap(): Map[String, Long]
  def collectFileToMinMax(cols: Seq[StructField]): Map[String, Map[String, Any]]

  def analyze(): Unit = {
    val (numericCols, nonNumericCols) = findNumericField(sparkSession, schema, targetCols)
    writeSkipMsgForNonNumeric(writer, nonNumericCols)
    val fileToSizeMap = collectFileToSizeMap()
    val fileToMinMaxMap = collectFileToMinMax(numericCols)
    numericCols.foreach { col =>
      val res = analyzeMinMaxHistogram(col, fileToSizeMap, fileToMinMaxMap)
      writer.appendResult(res)
    }
  }

  def analyzeMinMaxHistogram(
      col: StructField,
      fileToSizeMap: Map[String, Long],
      fileToMinMaxMap: Map[String, Map[String, Any]]): MinMaxAnalysisResult = {
    val ordering = TypeUtils.getInterpretedOrdering(col.dataType)
    val numeric = TypeUtils.getNumeric(col.dataType)
    val rawRange = fileToMinMaxMap
      .flatMap {
        case (fileName, stats) =>
          val fileByteSize = fileToSizeMap(fileName)
          val minVal = stats(s"min(${col.name})")
          val maxVal = stats(s"max(${col.name})")
          if (minVal == null) {
            // Ignore if min and max values are null. It means all values are null in the file.
            Nil
          } else {
            // Populate start/end marker for line sweep algorithm.
            // Contain both min/max value for range histogram and file size for estimation.
            // start marker: (min value, file size in bytes, max value, true for start)
            // end marker: (max value, file size in bytes, min value, false for end)
            (minVal, fileByteSize, maxVal, true) :: (maxVal, fileByteSize, minVal, false) :: Nil
          }
      }
      .toSeq
      // Sort by the first column and if same, end marker should come first.
      .sortWith(
        (a, b) => if (ordering.equiv(a._1, b._1)) a._4 > b._4 else ordering.lt(a._1, b._1))

    val totalBytes = fileToSizeMap.values.sum
    if (rawRange.isEmpty) {
      // All values are null.
      MinMaxAnalysisResult.allNullValueResult(col.name, fileToMinMaxMap.size, totalBytes)
    } else {

      val minVal = rawRange.head._1
      val maxVal = rawRange.last._1

      val range = rawRange.map { v =>
        (
          normalizeDouble(numeric.toDouble(v._1)) / 2.0,
          v._2,
          normalizeDouble(numeric.toDouble(v._3)) / 2.0,
          v._4)
      }
      val minDouble = range.head._1
      val distFigureLen = writer.distFigureWidth
      val unit = (range.last._1 - minDouble) / distFigureLen.toDouble

      var curNumFiles = 0
      var curBytes = 0L
      var maxBytesToLookup = 0L
      val unitHistogram = mutable.ArraySeq.fill(distFigureLen) {
        0
      }
      val maxUnitHistogram = mutable.ArraySeq.fill(distFigureLen) {
        0
      }
      range.foreach { r =>
        if (r._4) {
          // start marker
          curNumFiles += 1
          curBytes += r._2

          val startUnit = ((r._1 - minDouble) / unit).toInt
          val endUnit = ((r._3 - minDouble) / unit).toInt + 1
          val endUnitFixed = endUnit.min(distFigureLen)
          val startUnitFixed = startUnit.min(distFigureLen - 1)
          (startUnitFixed until endUnitFixed).foreach { i =>
            unitHistogram(i) += 1
            maxUnitHistogram(i) = maxUnitHistogram(i).max(unitHistogram(i))
          }
        } else {
          // end marker
          curNumFiles -= 1
          curBytes -= r._2

          // update histogram
          val startUnit = ((r._3 - minDouble) / unit).toInt
          val endUnit = ((r._1 - minDouble) / unit).toInt + 1
          val endUnitFixed = endUnit.min(distFigureLen)
          val startUnitFixed = startUnit.min(distFigureLen - 1)
          (startUnitFixed until endUnitFixed).foreach { i =>
            unitHistogram(i) -= 1
          }
        }
        maxBytesToLookup = maxBytesToLookup.max(curBytes)
      }

      MinMaxAnalysisResult(
        col.name,
        maxUnitHistogram,
        minVal.toString,
        maxVal.toString,
        fileToSizeMap.size,
        totalBytes,
        maxBytesToLookup)
    }
  }
}

case class DataframeMinMaxAnalyzer(
    override val writer: MinMaxResultWriter,
    override val targetCols: Seq[String],
    df: DataFrame)
    extends MinMaxAnalyzer {
  lazy override val schema = df.schema
  lazy override val sparkSession = df.sparkSession
  def collectFileToSizeMap(): Map[String, Long] = {
    val conf = df.sparkSession.sessionState.newHadoopConf()
    val fileSystem = new Path(df.inputFiles.head).getFileSystem(conf)
    val inputFiles = df.inputFiles
    inputFiles.par
      .map { path =>
        path -> fileSystem.getContentSummary(new Path(path)).getLength
      }
      .toIndexedSeq
      .toMap
  }

  def collectFileToMinMax(cols: Seq[StructField]): Map[String, Map[String, Any]] = {
    collectMinMaxFromDf(df, cols)
  }
}

trait MinMaxAnalysis {
  protected def analyzeDataframe(df: DataFrame, colNames: Seq[String], format: String): String = {
    val writer = MinMaxResultWriter.getWriter(format)
    val analyzer = DataframeMinMaxAnalyzer(writer, colNames, df)
    analyzer.analyze()
    writer.stringBuilder.mkString
  }
}

object MinMaxAnalysisUtil extends MinMaxAnalysis {

  /**
   * Analyze data layout based on min/max value for each file.
   */
  def analyze(df: DataFrame, colNames: Seq[String], format: String): String = {
    analyzeDataframe(df, colNames, format)
  }

  def analyze(df: DataFrame, colNames: Seq[String]): String = {
    analyze(df, colNames, "text")
  }
}
