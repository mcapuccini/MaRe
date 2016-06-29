package se.uu.farmbio.easymr

import java.io.File
import java.io.PrintWriter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source
import com.google.common.io.Files
import sys.process._
import org.apache.spark.Logging
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }
import scala.concurrent.Await
import scala.concurrent.duration._

class RunException(msg: String) extends Exception(msg)

object RunUtils extends Logging {

  def writeToFifo(fifo: File, toWrite: String) = {
    Future {
      new PrintWriter(fifo) {
        append(toWrite)
        close
      }
    } onFailure {
      case (e) => e.printStackTrace
    }
  }

  def readFromFifo(fifo: File, timeoutSec: Int) = {
    logInfo("reading output from fifo...")
    val future = Future {
      Source.fromFile(fifo).mkString
    } 
    Await.result(future, timeoutSec seconds)
  }

  def dockerRun(
    cmd: String,
    imageName: String,
    dockerOpts: String) = {
    command(s"docker run $dockerOpts $imageName sh -c ".split(" ") ++ Seq(cmd))
  }

  def mkfifo(name: String) = {
    val tmpDir = Files.createTempDir
    tmpDir.deleteOnExit
    val fifoPath = tmpDir.getAbsolutePath + s"/$name"
    val future = command(Seq("mkfifo", fifoPath), asynch = false)
    new File(fifoPath)
  }

  def command(cmd: Seq[String], asynch: Boolean = true) = {
    val future = Future {
      cmd ! ProcessLogger(
        (o: String) => logInfo(o),
        (e: String) => logInfo(e))
    }
    future onComplete {
      case Success(exitCode) => {
        if (exitCode != 0) {
          throw new RunException(s"${cmd.mkString(" ")} exited with non-zero exit code: $exitCode")
        } else {
          logInfo(s"successfully executed command: ${cmd.mkString(" ")}")
        }
      }
      case Failure(e) => e.printStackTrace
    }
    if (!asynch) {
      Await.ready(future, Duration.Inf)
    }
  }

}