package com.example
package clustering

import scala.collection.mutable.ListBuffer
import scala.util.Random


class KMeans {




}

object Utils {
  def generateData(): ListBuffer[(Double, Double)] = {
    val list = ListBuffer[(Double, Double)]()
    val rand = new Random()
    for (_ <- 1 to 10000) {
      val x = rand.nextDouble()
      val y = rand.nextDouble()
      list += ((x, y))
    }
    list
  }
}