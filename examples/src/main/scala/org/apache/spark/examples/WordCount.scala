package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/** Created by Feiyu on 2015/9/22. **/

object WordCount {
  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val partitionNumber = if(args.isDefinedAt(1)) args(1).toInt else 0
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))

    if(partitionNumber==0)
      line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
    else
      line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _,partitionNumber).collect()

    sc.stop()
  }
}
