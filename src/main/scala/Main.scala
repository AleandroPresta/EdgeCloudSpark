package com.example

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

    // Limiting the core usage to two, this wat we have better data and task distribution
    val conf = new SparkConf()
      .setAppName("EdgeCloud Example with 3 edge workers and 1 cloud workers")
      //.setMaster("spark://spark-master:7077")
      //.setMaster("local")
      .set("spark.locality.wait", "900000")
      .set("spark.executors.cores", "1")
      .set("spark.hadoop.dfs.replication", "2")
      .set("spark.hadoop.dfs.block.size", "1048576")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new TaskStartListener())

    val hdfsIp = "172.28.1.2"
    val hdfsPort = "8020"
    val hdfsPrefix = "hdfs://" + hdfsIp + ":" + hdfsPort

    println("\nEdge Phase\n")

    val userName = "aleandro"
    val fileName = "edgeData2.txt"
    val data: RDD[String] = readFromHDFS(sc = sc, hdfsPrefix = hdfsPrefix, userName = userName, fileName = fileName)

    // Perform Edge operations
    val edgeResults = edgePhase(data)

    println("\nCloud Phase\n")

    // Create RDD with the results of the Edge phase but with the cloud node as preferred location
    val cloudNodes: Seq[String] = Seq("cloud-worker1")

    val cloudRDD: RDD[Array[Int]] = createRDD(sc = sc, data = edgeResults, nodes = cloudNodes)

    // Cloud operations
    val cloudResult = cloudRDD.map(
      array => array.map(_ - 3)
    )

    // Collect the results
    val cloudData = cloudResult.collect()

    println("\nDone\n")

  }

  def readFromHDFS(sc: SparkContext, hdfsPrefix: String, userName: String, fileName: String) = {
    // Read from HDFS
    println(s"\nReading data from HDFS at $hdfsPrefix\n")
    val data = sc.textFile(hdfsPrefix + "/user/" + userName + "/" + fileName)

    // Print the number of partitions
    val numPartitions = data.getNumPartitions
    println(s"\nnumPartitions: $numPartitions\n")

    data
  }

  def edgePhase(data: RDD[String]) = {
    data.map(_ + 1)

    val edgeResults = data.collect().map(_.toInt)

    edgeResults
  }

  def createRDD(sc: SparkContext, data: Array[Int], nodes: Seq[String]) = {

    // Create RDD with preferred locations
    val tuple: (Array[Int], Seq[String]) = (data, nodes)
    val sequence: Seq[(Array[Int], Seq[String])] = Seq(tuple)

    val rdd = sc.makeRDD(sequence)

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

}