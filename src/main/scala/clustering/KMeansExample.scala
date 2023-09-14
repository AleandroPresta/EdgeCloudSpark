package com.example
package clustering

object KMeansExample {
  def main(args: Array[String]): Unit = {

    val size = 50
    val data = Utils.generateData(size)

    val k = 2
    val maxIterations = 100

    val kmeans = new KMeans(data, k, maxIterations)
    val clusters = kmeans.fit()

    println("Cluster assignments:")
    println(clusters.mkString(", "))
  }
}

