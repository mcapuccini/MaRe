package easymr

import com.google.common.io.Files
import scala.collection.JavaConverters.seqAsJavaListConverter
import java.io.PrintWriter
import scala.io.Source
import java.io.File

private[easymr] object RunUtils {
  
  def mkfifo = {
    val tmpDir = Files.createTempDir
    tmpDir.deleteOnExit
    val fifoPath = tmpDir.getAbsolutePath 
    RunUtils.command(s"mkfifo $fifoPath")
    new File(fifoPath)
  }
  
  def command(command: String, toPipe: String = "") = {
    //Format command
    val commandList = s"sh -c '$command'"
      .split(" ")
      .filter(_ != '\n')
      .toList
      .asJava
    //Start executable
    val pb = new ProcessBuilder(commandList)
    val proc = pb.start
    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader") {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start
    // Start a thread to feed the process input 
    new Thread("stdin writer") {
      override def run() {
        val out = new PrintWriter(proc.getOutputStream)
        out.println(toPipe)
        out.close()
      }
    }.start
    //Return results as a single string
    Source.fromInputStream(proc.getInputStream).mkString
  }

}