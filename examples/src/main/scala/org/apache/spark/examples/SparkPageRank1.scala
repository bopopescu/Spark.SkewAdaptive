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

import java.io.{File, FileWriter}
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 */
object SparkPageRank1 {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkPageRank <file> <iter> <partitionNumber> <SleepTimeSeconds> <openDebug> <closeCache> <closeDistinct>")
      System.exit(1)
    }

    showWarning()

    val isCacheClose = if(args.isDefinedAt(5)) args(5).toBoolean else false
    val isDistinctClose = if(args.isDefinedAt(6)) args(6).toBoolean else false
    val partitionNumber = args(2).toInt
    val iters = if (args.length > 0) args(1).toInt else 10
    val sparkConf = new SparkConf().setAppName(s"PageRank(Partition$partitionNumber,Iterator$iters)")

    val debug = if(args.isDefinedAt(4) && args(4).toBoolean) "y" else "n"
    println(s"open Debug . suspend = $debug .closeDistinct : $isDistinctClose . closeCache: $isCacheClose")
    sparkConf.set("spark.executor.extraJavaOptions", s"-Xdebug -Xrunjdwp:transport=dt_socket,address=8003,server=y,suspend=$debug")


    val sleepTime = if (args.isDefinedAt(3)) args(3).toInt else 0 //val rePartition = if (args.length > 4) args(4).toBoolean else false
    val ctx = new SparkContext(sparkConf)
    if (sleepTime > 0) {
      println(s"Waiting For ${sleepTime * 1000} seconds")
      Thread.sleep(sleepTime * 1000)
    }
    //8.30 textFile(路径,minPartitions),原先为1。
    //8.30 RDD的getPartitions都是调用上级RDD的partitions，所以在HadoopRDD中指定partition数量，在inputSplit中按照数量切开
    val lines = ctx.textFile(args(0), partitionNumber)
    val linksTemp = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }
    var links = if(isDistinctClose) linksTemp.groupByKey(partitionNumber)
                else linksTemp.distinct(partitionNumber).groupByKey(partitionNumber)

    links = if(isCacheClose) links else links.cache()

    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks, partitionNumber).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _, partitionNumber).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()

    val date = Calendar.getInstance()
    val file = new File(s"c:\\Result-PageRank[Part$partitionNumber,Iter$iters]-${date.get(java.util.Calendar.YEAR)}-${date.get(java.util.Calendar.MONTH) + 1}-${date.get(java.util.Calendar.DAY_OF_MONTH)}.txt")
    file.createNewFile()
    val writer = new FileWriter(file)
    output.foreach(tup => writer.append(tup._1 + " has rank: " + tup._2 + ".\n"))
    writer.flush()
    writer.close()
    //output.foreach(tup => print(tup._1 + " has rank: " + tup._2 + ".\t"))

    ctx.stop()
  }
}
