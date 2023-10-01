package com.example
package clustering

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random

case class KMeans(data: RDD[(Double, Double)], k: Int, maxIterations: Int) {

  var centroids: Array[(Double, Double)] = Array.empty

  // Initialize centroids randomly
  def initializeCentroids(): Unit = {
    centroids = data.takeSample(withReplacement = false, k, Random.nextLong())
  }

  // Assign each point to the nearest centroid
  def assignToCentroids(): RDD[(Int, (Double, Double))] = {
    data.map { point =>
      val (x, y) = point
      val closestCentroid = centroids.minBy { case (cx, cy) =>
        math.sqrt((x - cx) * (x - cx) + (y - cy) * (y - cy))
      }
      (centroids.indexOf(closestCentroid), point)
    }
  }

  // Update centroids based on the assigned points
  def updateCentroids(assignedPoints: RDD[(Int, (Double, Double))]): Unit = {
    centroids = assignedPoints
      .groupByKey()
      .mapValues { points =>
        val (sumX, sumY) = points.foldLeft((0.0, 0.0)) {
          case ((accX, accY), (x, y)) => (accX + x, accY + y)
        }
        (sumX / points.size, sumY / points.size)
      }
      .collect()
      .sortBy(_._1)
      .map(_._2)
  }

  // Run K-Means clustering
  def run(): Array[(Double, Double)] = {
    initializeCentroids()

    for (iteration <- 1 to maxIterations) {
      val assignedPoints = assignToCentroids() // Operazione Edge
      updateCentroids(assignedPoints) // Operazione Cloud
    }
    centroids
  }
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
