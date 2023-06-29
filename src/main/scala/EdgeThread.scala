package com.example

import org.apache.spark.SparkContext

import java.io.PrintWriter

class EdgeThread(sc: SparkContext, data: Array[Int], nodes: Seq[String], index: Int) extends Thread {

  override def run(): Unit = {

    println(s"\nEdge Thread $index starting")
    val tuple: (Array[Int], Seq[String]) = (data, nodes)
    val sequence: Seq[(Array[Int], Seq[String])] = Seq(tuple)

    val rdd = sc.makeRDD(sequence)

    val numSplits = rdd.getNumPartitions

    // Stampare le info dei job di questo thread
    val statusTracker = sc.statusTracker
    val info = statusTracker.getJobInfo(0)
    println(s"\n${info.mkString(", ")}")

    for (partitionId <- 0 until numSplits) {
      val preferredLocations = rdd.preferredLocations(rdd.partitions(partitionId))
      val p_id_print = partitionId+1 //è necessario incrementare di 1 il partitionID per stampare il numero corretto
      println(s"\nPartition $p_id_print / $numSplits, preferred locations: ${preferredLocations}")
    }

    // Operazione Edge
    val map_result = rdd.map(
      array => array.map(_ + 1)
    )

    val dataArray = map_result.collect()
    // Stampa risultati
    dataArray.foreach(array => println(array.mkString(", ")))

    val filePath = "/home/aleandro/IdeaProjects/SparkTest/src/main/scala/out/data" + index + ".txt"
    println(s"\nSaving result of Thread $index in $filePath")
    // Save to file
    val writer = new PrintWriter(filePath)
    dataArray.foreach(array => array.map(element => writer.println(element)))
    writer.close()
    println(s"\nEdge Thread $index ending")
  }

}
