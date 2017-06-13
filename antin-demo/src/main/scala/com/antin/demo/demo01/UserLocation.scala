package com.antin.demo.demo01

import org.apache.spark.{SparkConf, SparkContext}


object UserLocation {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MoblieLocation").setMaster("local[2]")
//      .setJars(Array("F:\\CommonDevelop\\hadoop\\project\\spark\\HelloSpark\\target\\hello-spark-1.0.jar"))
//      .setMaster("spark://192.168.2.88:7077")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("F:\\CommonDevelop\\hadoop\\testData\\bs_log").map(x => {
      val arr = x.split(",")
      val mb = (arr(0), arr(2)) //（手机号，基站）
      val flag = arr(3)
      var time = arr(1).toLong
      if (flag == "1") time = -time
      (mb, time)
    })
    val rdd2 = rdd1.reduceByKey(_ + _) //（（手机号，基站），停留时间）

    val rdd3 = sc.textFile("F:\\CommonDevelop\\hadoop\\testData\\loc_info.txt").map(x => {
      val arr = x.split(",")
      val bs = arr(0)
      (bs, (arr(1), arr(2))) //（基站，（经度，纬度））
    })

    val rdd4 = rdd2.map(t => (t._1._2, (t._1._1, t._2))) //（基站，（手机号，停留时间））

    val rdd5 = rdd4.join(rdd3) //(CC0710CC94ECC657A8561DE549D940E0,((18688888888,1300),(116.303955,40.041935))

    //    val rdd7 = rdd2.map(t => (t._1._1, t._1._2, t._2)).groupBy(_._1).values //CompactBuffer((18688888888,CC0710CC94ECC657A8561DE549D940E0,1300), (18688888888,9F36407EAD0629FC166F14DDE7970F68,51200), (18688888888,16030401EAFB68F1E3CDF819735E1C66,87600))
    //    println(rdd7.collect.toBuffer)

    val rdd6 = rdd2.map(t => (t._1._1, t._1._2, t._2)).groupBy(_._1).values.map(it => {
      it.toList.sortBy(_._3).reverse
    })
    //println(rdd6.collect.toBuffer)//每个手机号在哪个基站停留时间最长
    println(rdd5.collect.toBuffer)

  }
}
