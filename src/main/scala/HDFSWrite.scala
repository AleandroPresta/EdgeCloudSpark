package com.example

import org.apache.spark.{SparkConf, SparkContext}

case object HDFSWrite {
  def main(args: Array[String]): Unit = {

    // Limiting the core usage to two, this wat we have better data and task distribution
    val conf = new SparkConf()
      .setAppName("Write HDFS Example")
      .setMaster("local")
      //.setMaster("spark://spark-master:7077")
      //.set("spark.locality.wait", "900000")
      //.set("spark.executors.cores", "2")
    val sc = new SparkContext(conf)

    println("\nWriting\n")

    val prefix = "hdfs://172.19.0.3:9000"

    conf.set("fs.defaultFS", prefix)

    try {
      val numbersRdd1 = sc.parallelize((1 to 10000).toList)
      numbersRdd1.saveAsTextFile(prefix + "/user/aleandro961/ECS/data/data6.txt")
      println(s"RDD class: ${numbersRdd1.getClass}")
    } catch {
      case ex: org.apache.hadoop.mapred.FileAlreadyExistsException =>
        println("File1 already exists!")
    }
  }
}
