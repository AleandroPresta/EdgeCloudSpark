package com.example

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskStart}
import org.apache.spark.sql.SparkSession

class TaskStartListener1 extends SparkListener {
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

object MainFunzionante {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    spark.sparkContext.addSparkListener(new TaskStartListener())

    println(s"===============Initializing spark program===============")

    // Create two arrays of data
    val data1 = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println("Dati iniziali")
    println(data1.mkString(", "))


    // Definiamo gli IP dei nodi su cui mandare i dati
    val edge_nodes: Seq[String] = Seq("172.18.0.3", "172.18.0.4")
    val cloud_nodes: Seq[String] = Seq("172.18.0.5", "172.18.0.6")

    println(s"===============Fase 1===============")

    // Fase 1
    // Si incapsulano i dati e la destinazione in una Seq contenente una tupla
    val tuple1: (Array[Int], Seq[String]) = (data1, edge_nodes)
    val sequence1: Seq[(Array[Int], Seq[String])] = Seq(tuple1)

    val rdd = spark.sparkContext.makeRDD(sequence1) // Questo RDD contiene Array[Array[int]]
    val resultMap1: Array[Array[Int]] = rdd.map(
      array => array.map(_ - 1)
    ).collect()
    val data2: Array[Int] = resultMap1(0)
    println("Risultato fase 1")
    println(data2.mkString(", "))

    println(s"===============Fase 2================")

    // Fase 2
    val tuple2: (Array[Int], Seq[String]) = (data2, cloud_nodes)
    val sequence2: Seq[(Array[Int], Seq[String])] = Seq(tuple2)

    val rdd2 = spark.sparkContext.makeRDD(sequence2)
    val resultMap2: Array[Array[Int]] = rdd2.map(
      array => array.map(_ + 1)
    ).collect()
    val data3: Array[Int] = resultMap2(0)

    println("Risultato fase 2")
    println(data3.mkString(", "))

    println(s"===============Fase 3===============")

    // Fase 3
    val tuple3: (Array[Int], Seq[String]) = (data3, edge_nodes)
    val sequence3: Seq[(Array[Int], Seq[String])] = Seq(tuple3)

    val rdd3 = spark.sparkContext.makeRDD(sequence3)
    val resultMap3: Array[Array[Int]] = rdd3.map(
      array => array.map(_ + 3)
    ).collect()
    val data4: Array[Int] = resultMap3(0)
    println("Risultato fase 3")
    println(data4.mkString(", "))


    println(s"===============Terminating spark program===============")

    spark.stop()
  }
}