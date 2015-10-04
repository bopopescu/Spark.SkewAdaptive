/** Created by Feiyu on 2015/10/4. **/

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

package org.apache.spark.examples

import java.util.Random

import scala.math.exp

import breeze.linalg.{Vector, DenseVector}
import org.apache.hadoop.conf.Configuration

import org.apache.spark._
import org.apache.spark.scheduler.InputFormatInfo


/**
 * Logistic regression based classification.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
 * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS based on your needs.
 */
object SparkHdfsLR1 {
  //var D = 10   // Numer of dimensions
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String,D: Int): DataPoint = {
    /*val tok = new java.util.StringTokenizer(line.trim, "\\s+")
    var y = tok.nextToken.toDouble
    var x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble; i += 1
    }
    DataPoint(new DenseVector(x), y)*/
    val tok = line.trim.split("\\s+")
    //System.err.println(tok(0)+","+tok(1)+"   D:"+D)
    var y = tok(0).toDouble
    var x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok(i).toDouble; i += 1
    }
    DataPoint(new DenseVector(x), y)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
        |org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: SparkHdfsLR <file> <iters> <partitionNumber> <dimensions>")
      System.exit(1)
    }

    showWarning()

    val dimensions = if(args.isDefinedAt(3)) args(3).toInt else 10
    //D = dimensions
    val partitionNumber = if(args.isDefinedAt(2)) args(2).toInt else 7
    val sparkConf = new SparkConf().setAppName("SparkHdfsLR1")
    val inputPath = args(0)
    val conf = new Configuration()
    val sc = new SparkContext(sparkConf,
      InputFormatInfo.computePreferredLocations(
        Seq(new InputFormatInfo(conf, classOf[org.apache.hadoop.mapred.TextInputFormat], inputPath))
      ))
    val lines = sc.textFile(inputPath,partitionNumber)
    val points = lines.map(line => parsePoint(line,dimensions))//.cache()
    val ITERATIONS = args(1).toInt

    // Initialize w to a random value
    var w = DenseVector.fill(dimensions){2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = points.map { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    sc.stop()
  }
}
