package com.antin.test

/**
  * Created by root on 2016/5/18.
  */
object TestHash {

  def main(args: Array[String]) {
    val key = "php.antin.cn"
    val mod = 5
    val rawMod = key.hashCode % mod
    val partNum = rawMod + (if (rawMod < 0) mod else 0)
    println(partNum)
  }

}