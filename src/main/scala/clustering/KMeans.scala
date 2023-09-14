package com.example
package clustering

import scala.collection.mutable.ListBuffer
import scala.util.Random


class KMeans {




}

object Utils {
  def generateData(size: Int): ListBuffer[(Double, Double)] = {
    val list = ListBuffer[(Double, Double)]()
    val rand = new Random()
    for (_ <- 1 to size) {
      val x = rand.nextDouble()
      val y = rand.nextDouble()
      list += ((x, y))
    }
    list
  }
}