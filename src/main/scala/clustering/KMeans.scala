package com.example
package clustering

import scala.collection.mutable.ListBuffer
import scala.util.Random

class KMeans(data: ListBuffer[(Double, Double)], k: Int, maxIterations: Int) {
  // Number of data points
  private val numDataPoints = data.length

  // Initialize centroids with random values
  private var centroids: ListBuffer[(Double, Double)] = initializeCentroids()

  // Array to store cluster assignments for each data point
  private var clusters: Array[Int] = Array.fill(numDataPoints)(-1)

  // Function to initialize centroids with random values
  def initializeCentroids(): ListBuffer[(Double, Double)] = {
    val random = new Random()
    ListBuffer.fill(k)((random.nextDouble(), random.nextDouble()))
  }

  // Function to compute Euclidean distance between two points
  def euclideanDistance(point1: (Double, Double), point2: (Double, Double)): Double = {
    math.sqrt(math.pow(point1._1 - point2._1, 2) + math.pow(point1._2 - point2._2, 2))
  }

  // Assign each data point to the closest cluster centroid
  def assignToClusters(): Unit = {
    for (i <- 0 until numDataPoints) {
      val point = data(i)
      var minDistance = Double.MaxValue
      var closestCluster = -1

      for (j <- 0 until k) {
        val distance = euclideanDistance(point, centroids(j))
        if (distance < minDistance) {
          minDistance = distance
          closestCluster = j
        }
      }

      clusters(i) = closestCluster
    }
  }

  // Update the centroids based on the mean of data points in each cluster
  def updateCentroids(): Unit = {
    for (cluster <- 0 until k) {
      val clusterPoints = data.zip(clusters).collect { case (point, c) if c == cluster => point }
      if (clusterPoints.nonEmpty) {
        // Calculate the mean of x and y values separately for each cluster
        val (xSum, ySum) = clusterPoints.foldLeft((0.0, 0.0)) { case ((sumX, sumY), (x, y)) =>
          (sumX + x, sumY + y)
        }
        centroids(cluster) = (xSum / clusterPoints.length, ySum / clusterPoints.length)
      }
    }
  }

  // Fit the K-Means model and return the cluster assignments for each data point
  def fit(): Array[Int] = {
    for (iteration <- 1 to maxIterations) {
      assignToClusters()
      updateCentroids()
    }
    clusters
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