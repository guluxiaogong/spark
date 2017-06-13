package com.antin.demo.demo01

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "root")
    //非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WC")
      .setJars(Array("F:\\CommonDevelop\\hadoop\\project\\spark\\HelloSpark\\target\\hello-spark-1.0.jar"))
      .setMaster("spark://192.168.2.88:7077")
    val sc = new SparkContext(conf)

    //textFile会产生两个RDD：HadoopRDD  -> MapPartitinsRDD
    sc.textFile("hdfs://192.168.2.88:9000/input/test.log").cache()
      // 产生一个RDD ：MapPartitinsRDD
      .flatMap(_.split(" "))
      //产生一个RDD MapPartitionsRDD
      .map((_, 1))
      //产生一个RDD ShuffledRDD
      .reduceByKey(_ + _)
      //产生一个RDD: mapPartitions
      .saveAsTextFile("hdfs://192.168.2.88:9000/out001")

    sc.stop()
  }
}
