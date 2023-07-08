package com.example

import org.apache.spark.{SparkConf, SparkContext}

case object HDFSRead {
  def main(args: Array[String]): Unit = {

    // Limiting the core usage to two, this wat we have better data and task distribution
    val conf = new SparkConf()
      .setAppName("Read HDFS Example")
      .setMaster("spark://spark-master:7077")
      //.setMaster("local")
      //.set("spark.locality.wait", "900000")
      //.set("spark.executors.cores", "2")
    val sc = new SparkContext(conf)

    val prefix = "hdfs://172.19.0.2:9000"

    // conf.set("fs.defaultFS", prefix)

    println("\nReading\n")

    val lines = sc.textFile(prefix + "/user/aleandro961/ECS/data/data2.txt")
    val isEmpty = lines.isEmpty()
    println(s"\nEmpty : $isEmpty\n")
    lines.foreach(
      println
    )
  }
}
