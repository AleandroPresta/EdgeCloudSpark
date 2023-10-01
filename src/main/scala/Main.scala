package com.example

import clustering.KMeans

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskStart}
import org.apache.spark.{SparkConf, SparkContext}

class TaskStartListener extends SparkListener {
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val taskInfo = taskStart.taskInfo
    val taskId = taskInfo.taskId
    val executorId = taskInfo.executorId
    val taskHost = taskInfo.host
    println(s"\n\nTask started - Task ID: $taskId, Executor ID: $executorId, Host: $taskHost\n\n")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println(s"\n\nJob started - Job Properties:${jobStart.properties}")
  }
}

case object Main {
  def main(args: Array[String]): Unit = {

    // Defining parameters
    val hdfsIp = "172.28.1.2"
    val hdfsPort = "8020"
    val hdfsPrefix = "hdfs://" + hdfsIp + ":" + hdfsPort
    val cloudNodes: Seq[String] = Seq("cloud-worker1")
    val executorsCores = "1"
    val dfsReplication = "2"
    val userName = "aleandro"
    val fileName = "points.txt"

    val k = 2
    val maxIterations = 10

    // Print the parameters
    println(s"hdfsIp: $hdfsIp")
    println(s"hdfsPort: $hdfsPort")
    println(s"hdfsPrefix: $hdfsPrefix")
    println(s"cloudNodes: ${cloudNodes.mkString(", ")}")
    println(s"executorsCores: $executorsCores")
    println(s"dfsReplication: $dfsReplication")
    println(s"userName: $userName")
    println(s"fileName: $fileName")

    // Limiting the core usage to two, this wat we have better data and task distribution
    val conf = new SparkConf()
      .setAppName("EdgeCloud Example with 3 edge workers and 1 cloud workers")
      //.setMaster("spark://spark-master:7077")
      //.setMaster("local")
      .set("spark.locality.wait", "900000")
      .set("spark.executors.cores", executorsCores)
      .set("spark.hadoop.dfs.replication", dfsReplication)
      .set("spark.hadoop.dfs.block.size", "1048576")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new TaskStartListener())

    val data: RDD[(Double, Double)] = readFromHDFS(sc = sc, hdfsPrefix = hdfsPrefix, userName = userName, fileName = fileName)

    // printFirstElement(data)
    val kmeans: KMeans = KMeans(data, k, maxIterations)
    println("Initializing centroids")
    kmeans.initializeCentroids()

    val initialCentroids : Array[(Double, Double)] = kmeans.centroids
    // println("\nCentroids before iteration\n")
    // println(kmeans.centroids)

    for (i <- 1 to maxIterations) {
      println(s"Iteration $i")
      println("\nEdge Phase\n")

      // Perform Edge operations
      // Int, (Double, Double) indica la coppia id_centroide, (x_punto, y_punto)
      val edgeResults: Array[(Int, (Double, Double))] = edgePhase(data, kmeans).collect()

      // println("\nResults of the edge phase\n")
      // edgeResults.collect().foreach(println)

      println("\nCloud Phase\n")

      // Create RDD with the results of the Edge phase but with the cloud node as preferred location
      val cloudRDD: RDD[Array[(Int, (Double, Double))]] = createRDD(sc = sc, data = edgeResults, nodes = cloudNodes)

      cloudPhase(cloudRDD, kmeans)
      println(s"\nCentroids after iteration ${i}\n")
      kmeans.centroids.foreach(println)

    }

    println("\n-------------------------------------Finish!-------------------------------------\n")

    println("Data points:")
    data.collect().foreach(println)
    println("Initial centroids:")
    initialCentroids.foreach(println)
    val centroidsEc : Array[(Double, Double)] = kmeans.centroids
    println("Centroids:")
    centroidsEc.foreach(println)

  }

  def createRDD(sc: SparkContext, data: Array[(Int, (Double, Double))], nodes: Seq[String]): RDD[Array[(Int, (Double, Double))]] = {

    // Create RDD with preferred locations
    // Per usare le preferred locations bisogna accoppiare l'array di punti e cluster associato con le preferred locations
    val tuple: (Array[(Int, (Double, Double))], Seq[String]) = (data, nodes)
    // Per usare makeRDD bisogna incapsulare questo insieme di tuple in una sequenza
    val sequence: Seq[(Array[(Int, (Double, Double))], Seq[String])] = Seq(tuple)

    val rdd: RDD[Array[(Int, (Double, Double))]] = sc.makeRDD(sequence)

    val numSplits = rdd.getNumPartitions

    // Print job information
    val statusTracker = sc.statusTracker
    val info = statusTracker.getJobInfo(0)
    println(s"\n${info.mkString(", ")}")

    // Print preferred locations
    println(s"\nPrinting preferred locations of RDD")
    for (partitionId <- 0 until numSplits) {
      val preferredLocations = rdd.preferredLocations(rdd.partitions(partitionId))
      val p_id_print = partitionId + 1 //è necessario incrementare di 1 il partitionID per stampare il numero corretto
      println(s"\nPartition $p_id_print / $numSplits, preferred locations: ${preferredLocations}\n")
    }

    rdd
  }

  def readFromHDFS(sc: SparkContext, hdfsPrefix: String, userName: String, fileName: String) = {
    // Read from HDFS
    println(s"\nReading data from HDFS at $hdfsPrefix\n")
    val data: RDD[String] = sc.textFile(hdfsPrefix + "/user/" + userName + "/" + fileName)

    val dataElaborated = transform(data)

    // Print the number of partitions
    val numPartitions = data.getNumPartitions
    println(s"\nnumPartitions: $numPartitions\n")

    dataElaborated
  }

  def printFirstElement(data: RDD[(Double, Double)]): Unit = {
    val firstElement: (Double, Double) = data.first()

    val (element1, element2) = firstElement

    println(s"First Element: ($element1, $element2)")
  }

  def transform(inputRDD: RDD[String]): RDD[(Double, Double)] = {
    // Use map transformation to parse each input string and create (Double, Double) pairs
    val transformedRDD = inputRDD.map { inputString =>
      // Remove parentheses and split the string by comma
      val values = inputString.stripPrefix("(").stripSuffix(")").split(",")

      // Ensure that there are exactly two values and parse them as Doubles
      if (values.length == 2) {
        val x = values(0).trim.toDouble
        val y = values(1).trim.toDouble
        (x, y)
      } else {
        // If the format is incorrect, you can handle it as needed, e.g., by returning a default value or logging an error.
        (0.0, 0.0) // Default values if the format is incorrect
      }
    }

    transformedRDD
  }

  def edgePhase(data: RDD[(Double, Double)], kmeans: KMeans): RDD[(Int, (Double, Double))] = {

    val assignedPoints = kmeans.assignToCentroids()

    assignedPoints
  }

  def cloudPhase(data: RDD[Array[(Int, (Double, Double))]], kmeans: KMeans) = {
    // Cloud operations

    // Trasformare RDD[Array[(Int, (Double, Double))]] in RDD[(Int, (Double, Double))])
    val outputRDD: RDD[(Int, (Double, Double))] = data.flatMap(array => array)

    kmeans.updateCentroids(outputRDD)

  }

}