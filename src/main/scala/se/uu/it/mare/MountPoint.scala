package se.uu.it.mare

import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.util.regex.Pattern

import scala.io.Source

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
   * Appends a partition to a host path.
   *
   * @param hostPath host path where partition is written
   */
  def appendPartitionToHostPath(partition: Iterator[T], hostPath: File): Unit

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
 * @param recordDelimiter record delimiter (default: \n)
 * @param charset character encoding (default: StandardCharsets.UTF_8)
 */
case class TextFile(
  override val path: String,
  val recordDelimiter: String = "\n",
  val charset: String = "UTF-8")
  extends MountPoint[String](path) {

  def createEmptyMountPoint(hostPath: File): Unit = {
    hostPath.createNewFile
  }

  def writePartitionToHostPath(partition: Iterator[String], hostPath: File): Unit = {
    val fos = new FileOutputStream(hostPath)
    val osw = new OutputStreamWriter(fos, Charset.forName(charset))
    val pw = new PrintWriter(osw)
    partition.foreach(r => pw.write(r + recordDelimiter))
    pw.close
  }

  def appendPartitionToHostPath(partition: Iterator[String], hostPath: File): Unit = {
    val fos = new FileOutputStream(hostPath, true)
    val osw = new OutputStreamWriter(fos, Charset.forName(charset))
    val pw = new PrintWriter(osw)
    partition.foreach(r => pw.append(r + recordDelimiter))
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
 *  @param charset character encoding (default: StandardCharsets.UTF_8)
 */
case class WholeTextFiles(
  override val path: String,
  val charset: String = "UTF-8")
  extends MountPoint[(String, String)](path) {

  def createEmptyMountPoint(hostPath: File): Unit = {
    hostPath.mkdir
  }

  private def writePartitionToHostPath(
    partition: Iterator[(String, String)],
    hostPath: File,
    createEmptyMountPoint: Boolean): Unit = {
    if (createEmptyMountPoint) {
      this.createEmptyMountPoint(hostPath)
    }
    partition.foreach {
      case (filePath, text) =>
        val fileName = new File(filePath).getName
        val fos = new FileOutputStream(new File(hostPath, fileName))
        val osw = new OutputStreamWriter(fos, Charset.forName(charset))
        val pw = new PrintWriter(osw)
        pw.write(text)
        pw.close
    }
  }

  def writePartitionToHostPath(partition: Iterator[(String, String)], hostPath: File): Unit =
    this.writePartitionToHostPath(partition, hostPath, true)

  def appendPartitionToHostPath(partition: Iterator[(String, String)], hostPath: File): Unit =
    this.writePartitionToHostPath(partition, hostPath, false)

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

  private def writePartitionToHostPath(
    partition: Iterator[(String, Array[Byte])],
    hostPath: File,
    createEmptyMountPoint: Boolean): Unit = {
    if (createEmptyMountPoint) {
      this.createEmptyMountPoint(hostPath)
    }
    partition.foreach {
      case (filePath, bytes) =>
        val fileName = new File(filePath).getName
        Files.write(Paths.get(hostPath.getAbsolutePath, fileName), bytes)
    }
  }

  def writePartitionToHostPath(partition: Iterator[(String, Array[Byte])], hostPath: File): Unit =
    writePartitionToHostPath(partition, hostPath, true)

  def appendPartitionToHostPath(partition: Iterator[(String, Array[Byte])], hostPath: File): Unit =
    writePartitionToHostPath(partition, hostPath, false)

  def readPartitionFromHostPath(hostPath: File): Iterator[(String, Array[Byte])] = {
    val bytesSeq = hostPath.listFiles.map { file =>
      val bytes = Files.readAllBytes(Paths.get(file.getAbsolutePath))
      (file.getName, bytes)
    }
    bytesSeq.iterator
  }

}
