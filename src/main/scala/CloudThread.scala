package com.example

import org.apache.spark.SparkContext

class CloudThread(sc: SparkContext, data: Array[Int], nodes: Seq[String]) extends Thread {

  var results: Array[Int] = Array.empty[Int]

  override def run(): Unit = {

    println("\n---------- Cloud Thread Start ----------")

    val tuple: (Array[Int], Seq[String]) = (data, nodes)
    val sequence: Seq[(Array[Int], Seq[String])] = Seq(tuple)

    val rdd = sc.makeRDD(sequence)

    val numSplits = rdd.getNumPartitions

    // Stampare le info dei job di questo thread
    val statusTracker = sc.statusTracker
    val info = statusTracker.getJobInfo(0)
    println(s"\n${info.mkString(", ")}")

    println(s"\nPrinting preferred locations of RDD\n")
    for (partitionId <- 0 until numSplits) {
      val preferredLocations = rdd.preferredLocations(rdd.partitions(partitionId))
      val p_id_print = partitionId + 1 //è necessario incrementare di 1 il partitionID per stampare il numero corretto
      println(s"\nPartition $p_id_print / $numSplits, preferred locations: ${preferredLocations}\n")
    }

    // Operazione Edge
    val map_result = rdd.map(
      array => array.map(_ - 3)
    )

    val dataArray = map_result.collect()

    dataArray.foreach(
      array => array.foreach(
        element => results :+= element
      )
    )

  }

}
