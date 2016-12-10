package se.uu.it.easymr

import java.io.File
import java.io.PrintWriter
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.io.Source
import scala.sys.process.ProcessLogger
import scala.sys.process.stringSeqToProcess
import scala.util.Failure
import scala.util.Success

import com.google.common.io.Files
import org.apache.log4j.Logger


class RunException(msg: String) extends Exception(msg)

object RunUtils {
  
  val FIFO_READ_TIMEOUT = 1200
  private val THREAD_POOL_SIZE = 10
  
  def createThreadPool = {
    new ThreadPoolExecutor(
        THREAD_POOL_SIZE, 
        THREAD_POOL_SIZE, 
        0L, 
        TimeUnit.MILLISECONDS, 
        new LinkedBlockingQueue[Runnable], 
        Executors.defaultThreadFactory, 
        new DiscardPolicy)
  }
  
}

class RunUtils(val threadPool: ExecutorService) {
  
  implicit val ec = ExecutionContext.fromExecutor(threadPool)
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  def writeToFifo(fifo: File, toWrite: String) = {
    log.info(s"writing to fifo: ${fifo.getAbsolutePath}")
    Future {
      val pw = new PrintWriter(fifo)
      pw.write(toWrite)
      pw.close
    } onComplete {
      case Failure(e) => {
        log.warn(
            s"exeption while writing to ${fifo.getAbsolutePath} \n" + 
            e.getStackTraceString
        )
      }
      case Success(_) => log.info(s"successfully wrote into ${fifo.getAbsolutePath}")
    }
  }

  def readFromFifo(fifo: File, timeoutSec: Int) = {
    log.warn(s"reading output from fifo: ${fifo.getAbsolutePath}")
    val future = Future {
      Source.fromFile(fifo).mkString
    } 
    Await.result(future, timeoutSec seconds)
  }

  def dockerRun(
    cmd: String,
    imageName: String,
    dockerOpts: String,
    sudo: Boolean = false) = {
    val toRun = s"docker run --rm $dockerOpts $imageName sh -c ".split(" ") ++ Seq(cmd)
    val sudoStr = if(sudo) {
      command(Seq("sudo") ++ toRun)
    } else {
      command(toRun)
    }
  }

  def mkfifo(name: String) = {
    val tmpDir = Files.createTempDir
    tmpDir.deleteOnExit
    val fifoPath = tmpDir.getAbsolutePath + s"/$name"
    val future = command(Seq("mkfifo", fifoPath), asynch = false)
    val fifo = new File(fifoPath)
    fifo.deleteOnExit
    fifo
  }

  def command(cmd: Seq[String], asynch: Boolean = true) = {
    log.info(s"executing command: ${cmd.mkString(" ")}")
    val future = Future {
      cmd ! ProcessLogger(
        (o: String) => log.info(o),
        (e: String) => log.warn(e))
    }
    future onComplete {
      case Success(exitCode) => {
        if (exitCode != 0) {
          throw new RunException(s"${cmd.mkString(" ")} exited with non-zero exit code: $exitCode")
        } else {
          log.info(s"successfully executed command: ${cmd.mkString(" ")}")
        }
      }
      case Failure(e) => {
        log.warn(
            s"exeption while running ${cmd.mkString(" ")} \n" + 
            e.getStackTraceString
        )
      }
    }
    if (!asynch) {
      Await.ready(future, Duration.Inf)
    }
  }

}