package se.uu.it.easymr

import java.io.File
import java.io.PrintWriter
import java.util.UUID
import java.io.FileNotFoundException
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
  
  def writeToTmpFile(it: Iterator[String]): File = {
    val file = EasyFiles.newTmpFile
    val pw = new PrintWriter(file)
    it.foreach(pw.println)
    pw.close
    file
  }
  
}