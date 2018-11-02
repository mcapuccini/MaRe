package se.uu.it.mare

import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.InputStream
import java.io.PrintWriter
import java.util.UUID
import java.util.regex.Pattern

import scala.io.Source
import scala.util.Properties

import org.apache.log4j.Logger

private[mare] object FileHelper {

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
    val file = FileHelper.newTmpFile
    log.info(s"Creating new file: ${file.getAbsolutePath}")
    file.createNewFile
    file.deleteOnExit
    log.info(s"New file '${file.getAbsolutePath}' created")
    file
  }
  
  def createTmpDir = {
    val dir = FileHelper.newTmpFile
    log.info(s"Creating new directory: ${dir.getAbsolutePath}")
    dir.mkdir
    dir.deleteOnExit
    log.info(s"New directory '${dir.getAbsolutePath}' created")
    dir
  }

  def writeToTmpFile(it: Iterator[String], recordDelimiter: String) = {
    val file = FileHelper.newTmpFile
    log.info(s"Writing to: ${file.getAbsolutePath}")
    val pw = new PrintWriter(file)
    it.foreach(r => pw.write(r + recordDelimiter))
    pw.close
    log.info(s"Successfully wrote to: ${file.getAbsolutePath}")
    file
  }
  
  def writeToTmpDir(it: Iterator[(String,InputStream)]) = {
    val dir = FileHelper.createTmpDir
    log.info(s"Writing files to: ${dir.getAbsolutePath}")
    it.foreach { case(fileName,is) =>
      val file = new File(dir, fileName)
      val in = scala.io.Source.fromInputStream(is)
      val out = new PrintWriter(file)
      in.getLines().foreach(out.println(_))
      out.close
    }
    log.info(s"Successfully wrote files to: ${dir.getAbsolutePath}")
    dir
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
  
  def readFromDir(dir: File) = {
    log.info(s"Loading input streams from: ${dir.getAbsolutePath}")
    val isSeq = dir.listFiles.map { f =>
      (f.getName, new FileInputStream(f))
    }
    log.info(s"Successfully loaded input streams from: ${dir.getAbsolutePath}")
    isSeq
  }

}
