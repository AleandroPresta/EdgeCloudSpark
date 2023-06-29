package com.example

import java.nio.file._
import scala.collection.compat.IterableFactoryExtensionMethods
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.jdk.CollectionConverters.asScalaIteratorConverter

object DirectoryImporter {

  // Input: Riceve una directory di file testuali contenenti ognuno un array
  // Output: Un ListBuffer contenente Array numerici
  def import_directory(directoryPath: String): Array[Array[Int]] = {

    // val directoryPath: String = "/home/aleandro/IdeaProjects/SparkTest/src/main/scala/data"
    val fileExtension: String = ".txt" // Specify the desired file extension, or remove this line to include all files

    val directory: Path = Paths.get(directoryPath)

    val edge_paths: List[String] = Files.walk(directory)
      .filter(_.toFile.isFile)
      .filter(_.toString.endsWith(fileExtension))
      .iterator()
      .asScala
      .map(_.toString)
      .toList

    println(s"Scanning files inside $directory with extension $fileExtension")

    val edge_lines: ListBuffer[Array[Int]] = ListBuffer.empty[Array[Int]]

    for (i <- edge_paths.indices) {
      val lines: Array[Int] = Source.fromFile(edge_paths(i)).getLines.map(_.toInt).toArray
      edge_lines += ListBuffer.from(lines).toArray
    }

    return edge_lines.toArray

  }

}
