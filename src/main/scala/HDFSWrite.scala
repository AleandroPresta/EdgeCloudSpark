package com.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

case object HDFSWrite {
  def main(args: Array[String]): Unit = {

    // Limiting the core usage to two, this wat we have better data and task distribution
    val conf = new SparkConf()
      .setAppName("Write HDFS Example")
      .setMaster("local")
      .set("spark.hadoop.dfs.replication", "3")
      .set("spark.hadoop.dfs.block.size", "10048576")
    //.setMaster("spark://spark-master:7077")
    //.set("spark.locality.wait", "900000")
    //.set("spark.executors.cores", "2")
    val sc = new SparkContext(conf)

    println("\nWriting\n")

    try {
      val list = ListBuffer[Int]()
      val rand = new scala.util.Random()
      for (_ <- 1 to 10000) {
        val res0 = rand.nextInt()
        list += res0
      }
      println("Data sample:")
      println(list.take(5).mkString(", "))
      val conf = new Configuration()

      // Set the URI of the Hadoop cluster
      val hdfs = "hdfs://172.28.1.2:8020"
      val newReplicationFactor = 3
      conf.set("fs.defaultFS", hdfs)
      // Set the new replication factor in the configuration
      conf.set("dfs.replication", newReplicationFactor.toString)


      // Create a FileSystem object based on the configuration
      val fs = FileSystem.get(conf)

      val outputPath = hdfs + "/user/aleandro/edgeData2.txt"

      // Create a path object for the output file
      val outputPathObj = new Path(outputPath)

      // Create an output stream for the output file
      val outputStream = fs.create(outputPathObj)

      println("Saving array")
      // Iterate over the array and write each element to the output stream
      for (element <- list) {
        outputStream.writeBytes(element.toString)
        outputStream.writeBytes("\n") // Add a new line after each element
      }
      // Close the output stream
      outputStream.close()
    } catch {
      case ex: org.apache.hadoop.mapred.FileAlreadyExistsException =>
        println("File1 already exists!")
    }
  }
}