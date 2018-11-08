package se.uu.it.mare

import java.io.File
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths

import scala.io.Source

import java.util.regex.Pattern

/**
 * MountPoint defines how partitions are written to the a host path, and read back from it.
 *
 * @constructor
 * @param path mount point inside the in the Docker container
 */
abstract class MountPoint[T](val path: String) extends Serializable {

  /**
   * Creates an empty mount point at the specified host path.
   *
   * @param hostPath host path where the mount point is created
   */
  def createEmptyMountPoint(hostPath: File): Unit

  /**
   * Writes a partition to a host path.
   *
   * @param hostPath host path where partition is written
   */
  def writePartitionToHostPath(partition: Iterator[T], hostPath: File): Unit

  /**
   * Reads a partition from a host path.
   *
   * @param host path to read the partition from
   * @return partition iterator
   */
  def readPartitionFromHostPath(hostPath: File): Iterator[T]

}

/**
 *  TextFile mount point. Use this when processing large partitionable text files.
 *
 * @constructor
 * @param path mount point inside the in the Docker container
 * @param recordDelimiter record delimiter (default: \n).
 */
case class TextFile(override val path: String, val recordDelimiter: String = "\n")
  extends MountPoint[String](path) {

  def createEmptyMountPoint(hostPath: File): Unit = {
    hostPath.createNewFile
  }

  def writePartitionToHostPath(partition: Iterator[String], hostPath: File): Unit = {
    val pw = new PrintWriter(hostPath)
    partition.foreach(r => pw.write(r + recordDelimiter))
    pw.close
  }

  def readPartitionFromHostPath(hostPath: File): Iterator[String] = {
    val delimiterRegex = Pattern.quote(recordDelimiter)
    val source = Source.fromFile(hostPath)
    val records = source.mkString.split(delimiterRegex)
    source.close
    records.iterator
  }

}

/**
 *  WholeTextFiles mount point. Use this when processing many text files.
 */
case class WholeTextFiles(override val path: String) extends MountPoint[(String, String)](path) {

  def createEmptyMountPoint(hostPath: File): Unit = {
    hostPath.mkdir
  }

  def writePartitionToHostPath(partition: Iterator[(String, String)], hostPath: File): Unit = {
    this.createEmptyMountPoint(hostPath)
    partition.foreach {
      case (filePath, text) =>
        val fileName = new File(filePath).getName
        val pw = new PrintWriter(new File(hostPath, fileName))
        pw.write(text)
        pw.close
    }
  }

  def readPartitionFromHostPath(hostPath: File): Iterator[(String, String)] = {
    val fileSeq = hostPath.listFiles.map { file =>
      val source = Source.fromFile(file)
      val text = source.mkString
      source.close
      (file.getName, text)
    }
    fileSeq.iterator
  }

}

/**
 *  BinaryFiles mount point. Use this when processing many binary files.
 */
case class BinaryFiles(override val path: String) extends MountPoint[(String, Array[Byte])](path) {

  def createEmptyMountPoint(hostPath: File): Unit = {
    hostPath.mkdir
  }

  def writePartitionToHostPath(partition: Iterator[(String, Array[Byte])], hostPath: File): Unit = {
    this.createEmptyMountPoint(hostPath)
    partition.foreach {
      case (filePath, bytes) =>
        val fileName = new File(filePath).getName
        Files.write(Paths.get(hostPath.getAbsolutePath, fileName), bytes)
    }
  }

  def readPartitionFromHostPath(hostPath: File): Iterator[(String, Array[Byte])] = {
    val bytesSeq = hostPath.listFiles.map { file =>
      val bytes = Files.readAllBytes(Paths.get(file.getAbsolutePath))
      (file.getName, bytes)
    }
    bytesSeq.iterator
  }

}
