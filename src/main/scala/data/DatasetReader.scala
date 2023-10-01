package com.example
package data

import scala.collection.mutable.ListBuffer

object DatasetReader {
  def readData(csvFilePath: String): ListBuffer[(Double, Double)] = {
    val data = ListBuffer[(Double, Double)]()

    scala.io.Source.fromFile(csvFilePath).getLines().foreach { line =>
      val parts = line.split(",")
      if (parts.length == 3) { // Assuming CSV format: x-coordinate, y-coordinate, label
        try {
          val x = parts(0).toDouble
          val y = parts(1).toDouble
          data += ((x, y))
        } catch {
          case _: NumberFormatException => // Ignore lines with invalid data
        }
      }
    }

    data
  }

  def main(args: Array[String]): Unit = {
    // Replace 'your_csv_file.csv' with the path to your CSV file.
    val csvFilePath = "/home/aleandro/IdeaProjects/EdgeCloudSpark/src/main/scala/data/dataset.csv"

    // Read the data from the CSV file into a ListBuffer
    val data: ListBuffer[(Double, Double)] = readData(csvFilePath)

    // Print the first 10 elements of the ListBuffer
    data.take(10).foreach(println)
  }
}

