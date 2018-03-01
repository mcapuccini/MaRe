package se.uu.it.mare

import java.io.File
import java.io.FileNotFoundException
import java.io.PrintWriter
import java.util.UUID
import java.util.regex.Pattern

import scala.io.Source
import scala.util.Properties

import org.apache.log4j.Logger

private[mare] object EasyFiles {

  // Set temporary directory
  private val tmpDir = new File(Properties.envOrElse("TMPDIR", "/tmp"))
  if (!tmpDir.exists) {
    throw new FileNotFoundException(
      s"temporary directory ${tmpDir.getAbsolutePath} doesn't extist")
  }
  if (!tmpDir.isDirectory) {
    throw new FileNotFoundException(
      s"${tmpDir.getAbsolutePath} is not a directory")
  }
  
  // Logger
  private lazy val log = Logger.getLogger(getClass.getName)

  private def newTmpFile = new File(tmpDir, "mare_" + UUID.randomUUID.toString)

  def createTmpFile = {
    val file = EasyFiles.newTmpFile
    log.info(s"Creating new file: ${file.getAbsolutePath}")
    file.createNewFile
    log.info(s"New file '${file.getAbsolutePath}' created")
    file
  }

  def writeToTmpFile(it: Iterator[String], recordDelimiter: String): File = {
    val file = EasyFiles.newTmpFile
    log.info(s"Writing to: ${file.getAbsolutePath}")
    val pw = new PrintWriter(file)
    it.foreach(r => pw.write(r + recordDelimiter))
    pw.close
    log.info(s"Successfully wrote to: ${file.getAbsolutePath}")
    file
  }

  def readFromFile(file: File, recordDelimiter: String) = {
    val delimiterRegex = Pattern.quote(recordDelimiter)
    log.info(s"Reading from: ${file.getAbsolutePath}")
    val source = Source.fromFile(file)
    val recordsIteratior = source.mkString.split(delimiterRegex).iterator
    source.close
    log.info(s"Successfully read from: ${file.getAbsolutePath}")
    recordsIteratior
  }

}
