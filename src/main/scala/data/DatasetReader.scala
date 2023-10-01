package com.example
package data

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DatasetReader {
  def readRDD(csvFilePath: String, sc: SparkContext): RDD[(Double, Double)] = {
    val pointRDD: RDD[(Double, Double)] = sc.textFile(csvFilePath).flatMap { line =>
      val parts = line.split(",")
      if (parts.length == 3) { // Assuming CSV format: x-coordinate, y-coordinate, label
        try {
          val x = parts(0).toDouble
          val y = parts(1).toDouble
          Some((x, y))
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }
    pointRDD
  }

  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val conf = new SparkConf().setAppName("DatasetReader").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    // Replace 'your_csv_file.csv' with the path to your CSV file.
    val csvFilePath = "/home/aleandro/IdeaProjects/EdgeCloudSpark/src/main/scala/data/dataset.csv"

    // Read the RDD from the CSV file
    val pointRDD: RDD[(Double, Double)] = readRDD(csvFilePath, sc)

    // Collect and print the first 10 elements of the RDD
    pointRDD.take(10).foreach(println)

    // Stop Spark
    spark.stop()
  }
}
