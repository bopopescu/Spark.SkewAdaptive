package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/** Created by Feiyu on 2015/10/5. **/
object InvertedIndex {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkPageRank <file> <partitionNumber>")
      System.exit(1)
    }

    val partitionNumber = args(2).toInt
    val RecordsInEachDoc = if (args.length > 0) args(1).toInt else 10
    val sparkConf = new SparkConf().setAppName(s"InvertedIndex(Partition$partitionNumber)")

    val ctx = new SparkContext(sparkConf)

    //8.30 textFile(路径,minPartitions),原先为1。
    //8.30 RDD的getPartitions都是调用上级RDD的partitions，所以在HadoopRDD中指定partition数量，在inputSplit中按照数量切开
    val lines = ctx.textFile(args(0), partitionNumber)
    var recordId = 0L

    val data = lines.map(info => {
      val arr = info.trim.split("\\s+")
      (arr(0),arr(1))
    })
    val data1 = data.mapValues(i => 1)

    val data_docId = data.map(i => {
      recordId += 1
      (i._1,recordId / RecordsInEachDoc)
    })

    val data3 = data.reduceByKey((i,j) => i + j).groupByKey().join(data_docId, partitionNumber).join(data1,partitionNumber)
      .reduceByKey((i,j) => (i._1,i._2+j._2) , partitionNumber).collect()
    /*val data2 = data.groupByKey(partitionNumber).join(data_docId, partitionNumber)
      .reduceByKey((i,j) => (i._1,i._2+j._2) , partitionNumber).collect()*/

    ctx.stop()
  }
}
