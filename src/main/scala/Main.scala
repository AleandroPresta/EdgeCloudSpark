package com.example

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
    println(s"\nReading data from HDFS at $hdfsPrefix\n")
    val lines = sc.textFile(hdfsPrefix + "/user/aleandro/edgeData2.txt")

    val numPartitions = lines.getNumPartitions
    println(s"\nnumPartitions: $numPartitions\n")

    lines.map(_ + 1)

    val edgeResults = lines.collect().map(_.toInt)

    println("\nCloud Phase\n")
    val cloudNodes: Seq[String] = Seq("cloud-worker1")
    val tuple: (Array[Int], Seq[String]) = (edgeResults, cloudNodes)
    val sequence: Seq[(Array[Int], Seq[String])] = Seq(tuple)

    val rdd = sc.makeRDD(sequence)

    val numSplits = rdd.getNumPartitions

    // Stampare le info dei job di questo thread
    val statusTracker = sc.statusTracker
    val info = statusTracker.getJobInfo(0)
    println(s"\n${info.mkString(", ")}")

    println(s"\nPrinting preferred locations of RDD")
    for (partitionId <- 0 until numSplits) {
      val preferredLocations = rdd.preferredLocations(rdd.partitions(partitionId))
      val p_id_print = partitionId + 1 //è necessario incrementare di 1 il partitionID per stampare il numero corretto
      println(s"\nPartition $p_id_print / $numSplits, preferred locations: ${preferredLocations}\n")
    }

    // Operazione edge
    val cloudResult = rdd.map(
      array => array.map(_ - 3)
    )

    // Raccolta dei risultati
    val cloudData = cloudResult.collect()

    /*

      Soluzione proposta

      val hdfsCloudIp = "172.28.1.*"
      val hdfsCloudPort = "8020"
      val hdfsCloudPrefix = "hdfs://" + hdfsCloudIp + ":" + hdfsCloudPort

      Scrivere edgeResults su HDFS Cloud

      Eseguire le operazioni cloud

     */

    println("\nDone\n")

  }
}