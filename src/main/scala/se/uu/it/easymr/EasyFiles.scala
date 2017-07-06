package se.uu.it.easymr

import java.io.File
import java.io.FileNotFoundException
import java.io.PrintWriter
import java.util.UUID
import java.util.regex.Pattern

import scala.io.Source
import scala.util.Properties


private[easymr] object EasyFiles {
  
  // Set temporary directory
  private val tmpDir = new File(Properties.envOrElse("TMPDIR", "/tmp" ))
  if(!tmpDir.exists) {
    throw new FileNotFoundException(
        s"temporary directory ${tmpDir.getAbsolutePath} doesn't extist")
  }
  if(!tmpDir.isDirectory) {
    throw new FileNotFoundException(
        s"${tmpDir.getAbsolutePath} is not a directory")
  }
  
  private def newTmpFile = new File(tmpDir, "easymr_" + UUID.randomUUID.toString)
  
  def createTmpFile = {
    val file = EasyFiles.newTmpFile
    file.createNewFile
    file
  }
  
  def writeToTmpFile(it: Iterator[String], recordDelimiter: String): File = {
    val file = EasyFiles.newTmpFile
    val pw = new PrintWriter(file)
    it.foreach(r => pw.write(r+recordDelimiter))
    pw.close
    file
  }
  
  def readFromFile(file: File, recordDelimiter: String) = {    
    val delimiterRegex = Pattern.quote(recordDelimiter)
    val source = Source.fromFile(file)
    val recordsIteratior = source.mkString.split(delimiterRegex).iterator
    source.close
    recordsIteratior
  }
  
}