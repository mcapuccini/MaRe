package se.uu.it.mare

import java.io.File
import java.util.regex.Pattern

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

private object TextFileMaRe {

  // Logger
  private lazy val log = Logger.getLogger(getClass.getName)

  def mapLambda(
    imageName:        String,
    command:          String,
    inputMountPoint:  String,
    outputMountPoint: String,
    records:          Iterator[String],
    recordDelimiter:  String,
    forcePull:        Boolean) = {

    // Create temporary files
    val inputFile = FileHelper.writeToTmpFile(records, recordDelimiter)
    val outputFile = FileHelper.createTmpFile

    // Run docker
    DockerHelper.run(
      imageName,
      command,
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(new File(inputMountPoint), new File(outputMountPoint)),
      forcePull)

    // Retrieve output
    val output = FileHelper.readFromFile(outputFile, recordDelimiter)

    // Remove temporary files
    log.info(s"Deleteing temporary file: ${inputFile.getAbsolutePath}")
    inputFile.delete
    log.info(s"Temporary file '${inputFile.getAbsolutePath}' deleted successfully")
    log.info(s"Deleteing temporary file: ${outputFile.getAbsolutePath}")
    outputFile.delete
    log.info(s"Temporary file '${outputFile.getAbsolutePath}' deleted successfully")

    // Return output
    output

  }

}

/**
 * Text file MaRe implementation.
 *
 *  @constructor
 *  @param rdd input RDD
 *  @param inputMountPoint mount point for the input chunk that is passed to the containers
 *  @param outputMountPoint mount point where the processed data is read back to Spark
 */
class TextFileMaRe(
  private val rdd:      RDD[String],
  val inputMountPoint:  String      = "/input",
  val outputMountPoint: String      = "/output",
  val forcePull:        Boolean     = false) extends MaRe[String](rdd) {

  val recordDelimiter =
    Option(rdd.sparkContext.hadoopConfiguration.get("textinputformat.record.delimiter"))
      .getOrElse("\n")

  def cache = {
    new TextFileMaRe(
      rdd.cache,
      inputMountPoint,
      outputMountPoint)
  }

  def repartition(numPartitions: Int) = {
    new TextFileMaRe(
      rdd.repartition(numPartitions),
      inputMountPoint,
      outputMountPoint)
  }

  def setInputMountPoint(inputMountPoint: String) = {
    new TextFileMaRe(
      rdd,
      inputMountPoint,
      outputMountPoint)
  }

  def setOutputMountPoint(outputMountPoint: String) = {
    new TextFileMaRe(
      rdd,
      inputMountPoint,
      outputMountPoint)
  }

  def forcePull(forcePull: Boolean) = {
    new TextFileMaRe(
      rdd,
      inputMountPoint,
      outputMountPoint,
      forcePull)
  }

  def map(
    imageName: String,
    command:   String) = {

    // Map partitions to avoid opening too many files
    val resRDD = rdd.mapPartitions { records =>
      TextFileMaRe.mapLambda(
        imageName,
        command,
        inputMountPoint,
        outputMountPoint,
        records,
        recordDelimiter,
        forcePull)
    }
    new TextFileMaRe(
      resRDD,
      inputMountPoint,
      outputMountPoint)

  }

  def reduce(
    imageName: String,
    command:   String,
    depth:     Int    = 2): String = {

    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")

    val scale = math.max(math.ceil(math.pow(this.getNumPartitions, 1 / depth)).toInt, 2)
    if (depth > 2 && this.getNumPartitions > this.getNumPartitions / scale) {
      // If depth greater than 2 and partitions will scale down, map and repartition
      val reduced = this.map(imageName, command)
      reduced.repartition(reduced.getNumPartitions / scale)
        .reduce(imageName, command, depth - 1)
    } else if (this.getNumPartitions > 1) {
      // If depth is in {1,2} map, then collect and reduce locally
      val reduced = this.map(imageName, command)
      val records = reduced.getRDD.collect
      TextFileMaRe.mapLambda(
        imageName,
        command,
        inputMountPoint,
        outputMountPoint,
        records.iterator,
        recordDelimiter,
        forcePull)
        .map(_ + recordDelimiter)
        .mkString
    } else {
      // If there is only 1 partition it's better to reduce before to collect
      val reduced = this.map(imageName, command)
      val records = reduced.getRDD.collect
      records.map(_ + recordDelimiter).mkString
    }

  }

}
