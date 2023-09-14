package com.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

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

      val list = generateData();

      println("Data sample:")
      println(list.take(5).mkString(", "))

      val hdfsIp = "hdfs://172.28.1.2:8020"
      val replicationFactor = 3
      val fs = setUpHDFS(hdfsIp, replicationFactor)

      val outputPath = hdfsIp + "/user/aleandro/edgeData2.txt"

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

    def generateData(): ListBuffer[(Double, Double)] = {
      val list = ListBuffer[(Double, Double)]()
      val rand = new Random()
      for (_ <- 1 to 10000) {
        val x = rand.nextDouble()
        val y = rand.nextDouble()
        list += ((x, y))
      }
      list
    }

    def setUpHDFS(hdfsIp: String, replicationFactor: Int) = {
      // Setting HDFS
      val conf = new Configuration()

      conf.set("fs.defaultFS", hdfsIp)
      // Set the new replication factor in the configuration
      conf.set("dfs.replication", replicationFactor.toString)

      // Create a FileSystem object based on the configuration
      val fs = FileSystem.get(conf)

      fs
    }

}