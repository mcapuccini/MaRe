package se.uu.it.easymr

import java.io.File
import java.io.PrintWriter
import java.util.UUID

private[easymr] object EasyFiles {
  
  private val tmpDir = if(System.getenv("EASYMR_TMP") != null) {
    new File(System.getenv("EASYMR_TMP"))
  } else if (System.getenv("TMPDIR") != null) {
    new File(System.getenv("TMPDIR"))
  } else {
    val file = new File("/tmp")
    if(!f.exists() || !f.isDirectory()) { 
      throw new IllegalStateException(
        "Environment variables EASYMR_TMP and TMPDIR are not defined. " +
        "Please define at least one of the two."
      )
    }
    file
  }
    
  private def newTmpFile = new File(tmpDir, "easymr_" + UUID.randomUUID.toString)
  
  def createTmpFile = {
    val file = EasyFiles.newTmpFile
    file.createNewFile
    file
  }
  
  def writeToTmpFile(toWrite: String): File = {
    EasyFiles.writeToTmpFile(Seq(toWrite).iterator)
  }
  
  def writeToTmpFile(it: Iterator[String]): File = {
    val file = EasyFiles.newTmpFile
    val pw = new PrintWriter(file)
    while(it.hasNext) {
      val line = it.next
      if(it.hasNext) {
        pw.println(line)
      } else {
        pw.write(line)
      }
    }
    pw.close
    file
  }
  
}
