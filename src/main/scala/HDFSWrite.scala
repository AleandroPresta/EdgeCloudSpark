package com.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import clustering.Utils

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

      val size = 1000
      val points = Utils.generateData(size);

      println("Data sample:")
      println(points.take(5).mkString(", "))

      /* val hdfsIp = "hdfs://172.28.1.2:8020"
      val replicationFactor = 3
      val fs = setUpHDFS(hdfsIp, replicationFactor)

      val fileName = "points.txt"
      val userName = "aleandro"
      saveFile(points = points, fs = fs, hdfsIp = hdfsIp, fileName = fileName, userName = userName)

    } catch {
      case ex: org.apache.hadoop.mapred.FileAlreadyExistsException =>
        println("File1 already exists!")
    */ }

  }

  private def setUpHDFS(hdfsIp: String, replicationFactor: Int) = {
    // Setting HDFS
    val conf = new Configuration()

    conf.set("fs.defaultFS", hdfsIp)
    // Set the new replication factor in the configuration
    conf.set("dfs.replication", replicationFactor.toString)

    // Create a FileSystem object based on the configuration
    val fs = FileSystem.get(conf)

    fs
  }

  private def saveFile(points: ListBuffer[(Double, Double)], fs: FileSystem, hdfsIp: String, fileName: String, userName: String): Unit = {
    val outputPath = hdfsIp + "/user/" + userName + "/" + fileName

    // Create a path object for the output file
    val outputPathObj = new Path(outputPath)

    // Create an output stream for the output file
    val outputStream = fs.create(outputPathObj)

    println("Saving data")
    // Iterate over the array and write each element to the output stream
    for (element <- points) {
      outputStream.writeBytes(element.toString)
      outputStream.writeBytes("\n") // Add a new line after each element
    }
    // Close the output stream
    outputStream.close()

  }

}