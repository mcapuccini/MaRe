package se.uu.it.mare

import org.apache.spark.rdd.RDD
import scala.reflect.io.File
import org.apache.log4j.Logger

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import org.apache.commons.io.FileUtils

private object WholeTextFilesMaRe {
  
  // Logger
  private lazy val log = Logger.getLogger(getClass.getName)

  def mapLambda(
    imageName:        String,
    command:          String,
    inputMountPoint:  String,
    outputMountPoint: String,
    records:          Iterator[(String,String)],
    forcePull:        Boolean) = {

    // Create temporary files
    val isRecords = records.map{ case(fn,str) => 
      (fn, new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8)))
    }
    val inputDir = FileHelper.writeToTmpDir(isRecords)
    val outputDir = FileHelper.createTmpFile

    // Run docker
    DockerHelper.run(
      imageName,
      command,
      bindFiles = Seq(inputDir, outputDir),
      volumeFiles = Seq(new File(inputMountPoint), new File(outputMountPoint)),
      forcePull)

    // Retrieve output
    val output = FileHelper.readFromDir(outputDir)

    // Remove temporary dirs
    log.info(s"Deleteing temporary directory: ${inputDir.getAbsolutePath}")
    FileUtils.deleteDirectory(inputDir)
    log.info(s"Temporary directory '${inputDir.getAbsolutePath}' deleted successfully")
    log.info(s"Deleteing temporary directory: ${outputDir.getAbsolutePath}")
    FileUtils.deleteDirectory(outputDir)
    log.info(s"Temporary directory '${outputDir.getAbsolutePath}' deleted successfully")

    // Return output
    output

  }
  
}

class WholeTextFilesMaRe(
  private val rdd:      RDD[(String, String)],
  val inputMountPoint:  String                = "/input",
  val outputMountPoint: String                = "/output",
  val forcePull:        Boolean               = false) extends MaRe[(String, String)](rdd) {

  def cache = {
    new WholeTextFilesMaRe(
      rdd.cache,
      inputMountPoint,
      outputMountPoint)
  }

  def repartition(numPartitions: Int) = {
    new WholeTextFilesMaRe(
      rdd.repartition(numPartitions),
      inputMountPoint,
      outputMountPoint)
  }

  def setInputMountPoint(inputMountPoint: String) = {
    new WholeTextFilesMaRe(
      rdd,
      inputMountPoint,
      outputMountPoint)
  }

  def setOutputMountPoint(outputMountPoint: String) = {
    new WholeTextFilesMaRe(
      rdd,
      inputMountPoint,
      outputMountPoint)
  }

  def forcePull(forcePull: Boolean) = {
    new WholeTextFilesMaRe(
      rdd,
      inputMountPoint,
      outputMountPoint,
      forcePull)
  }
  
}