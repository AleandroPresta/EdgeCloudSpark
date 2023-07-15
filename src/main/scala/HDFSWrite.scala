package com.example

import org.apache.spark.{SparkConf, SparkContext}

case object HDFSWrite {
  def main(args: Array[String]): Unit = {

    // Limiting the core usage to two, this wat we have better data and task distribution
    val conf = new SparkConf()
      .setAppName("Write HDFS Example")
      .setMaster("local")
      .set("spark.hadoop.dfs.replication", "3")
      .set("spark.hadoop.dfs.block.size", "1048576")
      //.setMaster("spark://spark-master:7077")
      //.set("spark.locality.wait", "900000")
      //.set("spark.executors.cores", "2")
    val sc = new SparkContext(conf)

    println("\nWriting\n")

    val prefix = "hdfs://172.19.0.4:9000"

    conf.set("fs.defaultFS", prefix)

    try {
      val numbersRdd1 = sc.parallelize((1 to 10000000).toList)
      numbersRdd1.saveAsTextFile(prefix + "/EdgeCloud/data2.txt")
      println(s"RDD class: ${numbersRdd1.getClass}")
    } catch {
      case ex: org.apache.hadoop.mapred.FileAlreadyExistsException =>
        println("File1 already exists!")
    }
  }
}
