package com.example
package clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import clustering.KMeans

object KMeansExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KMeansExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Sample data as an RDD of (Double, Double) pairs
    val data: RDD[(Double, Double)] = sc.parallelize(Seq(
      (1.0, 2.0),
      (2.0, 3.0),
      (8.0, 7.0),
      (9.0, 8.0),
      // Add more data points here
    ))

    // Create a KMeans instance and run the clustering
    val k = 2
    val maxIterations = 10
    val kmeans: KMeans = KMeans(data, k, maxIterations)
    val centroids = kmeans.run()

    // Display the final centroids
    centroids.foreach(println)

    // Stop Spark context
    sc.stop()
  }
}

