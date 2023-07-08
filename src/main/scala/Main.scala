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
      .setAppName("Read HDFS Example")
      .setMaster("spark://spark-master:7077")
      //.setMaster("local")
      .set("spark.locality.wait", "900000")
      .set("spark.executors.cores", "1")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new TaskStartListener())

    val hdfs_ip = args(0)
    val hdfs_prefix = "hdfs://" + hdfs_ip + ":9000"

    val edgeNodes: Seq[String] = Seq("172.19.0.5", "172.19.0.6", "172.19.0.7")
    val cloudNodes: Seq[String] = Seq("172.19.0.8", "172.19.0.9", "172.19.0.10")

    println(s"\nReading data from HDFS at $hdfs_prefix\n")
    val lines = sc.textFile(hdfs_prefix + "/user/aleandro961/ECS/data/data6.txt")

    // Questa operazione avviene su dei nodi, su quale nodo deve avvenire?
    val inputData: Array[Int] = lines.collect().map(_.toInt)

    println("\ninputData sample: " + inputData.take(5).mkString(", ") + "\n")

    val edgeThread = new EdgeThread(sc = sc, data = inputData, nodes = edgeNodes)
    edgeThread.start()
    edgeThread.join()

    val cloudData = edgeThread.results
    println("\nResult sample of the edge phase " + cloudData.take(5).mkString(", "))

    val cloudThread = new CloudThread(sc = sc, data = cloudData, nodes = cloudNodes)
    cloudThread.start()
    cloudThread.join()

    val cloudResults = cloudThread.results
    println("\nResult sample of the cloud phase " + cloudResults.take(5).mkString(", "))

    println("\nDone\n")

  }
}
