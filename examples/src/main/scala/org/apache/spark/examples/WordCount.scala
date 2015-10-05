package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/** Created by Feiyu on 2015/9/22. **/

object WordCount {
  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: <file> <partitionNumber>")
      System.exit(1)
    }

    val partitionNumber = if(args.isDefinedAt(1)) args(1).toInt else 2
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0),partitionNumber)

    val size = line.count()
    line.flatMap(_.trim.split("\\s+")).map((_, 1)).reduceByKey(_ + _, partitionNumber).map(info => info._2/size).collect()

    sc.stop()
  }
}
