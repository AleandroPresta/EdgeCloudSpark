package com.example

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskStart}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

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

object Main {
  def main(args: Array[String]): Unit = {

    val edgeNodes: Seq[String] = Seq("172.18.0.3", "172.18.0.4", "172.18.0.5")
    val cloudNodes: Seq[String] = Seq("172.18.0.6", "172.18.0.7", "172.18.0.8")

    // Limiting the core usage to two, this wat we have better data and task distribution
    val conf = new SparkConf()
      .setAppName("Thread Refactoring Example")
      .set("spark.locality.wait", "900000").set("spark.executors.cores", "2")
    val sc = new SparkContext(conf)

    // Si usa uno spark listener per tracciare il lavoro di spark
    sc.addSparkListener(new TaskStartListener())

    // Fase edge, import dei dati
    val edgeDirectory = "/home/aleandro/IdeaProjects/EdgeCloudSpark/src/main/scala/data"
    val edgeLines: Array[Array[Int]] = DirectoryImporter.import_directory(directoryPath = edgeDirectory)

    // Inizializza, fa partire e aspetta la fine dei thread
    val edgeThreads = ListBuffer.empty[EdgeThread]

    for (i <- edgeLines.indices) {
      edgeThreads += new EdgeThread(sc = sc, edgeLines(i), edgeNodes, index = i)
    }

    edgeThreads.foreach(
      thread => thread.start()
    )

    edgeThreads.foreach(
      thread => thread.join()
    )

    // Fase Cloud
    val cloudDirectory = "/home/aleandro/IdeaProjects/EdgeCloudSpark/src/main/scala/out"
    val cloudLines: Array[Array[Int]] = DirectoryImporter.import_directory(directoryPath = cloudDirectory)

    val cloudThreads = ListBuffer.empty[CloudThread]

    for (i <- cloudLines.indices) {
      cloudThreads += new CloudThread(sc = sc, cloudLines(i), cloudNodes, index = i)
    }

    cloudThreads.foreach(
      thread => thread.start()
    )

    cloudThreads.foreach(
      thread => thread.join()
    )

    sc.stop()
  }
}