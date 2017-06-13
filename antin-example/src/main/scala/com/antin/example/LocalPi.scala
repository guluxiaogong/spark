package com.antin.example

import scala.math._

/**
  * Created by Administrator on 2017/5/26.
  */
object LocalPi {
  def main(args: Array[String]) {
    var count = 0
    for (i <- 1 to 100000) {
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) count += 1
    }
    println("Pi is roughly " + 4 * count / 100000.0)
  }
}
