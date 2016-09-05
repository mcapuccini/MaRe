package se.uu.farmbio.easymr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scopt.OptionParser
import org.apache.spark.Logging

case class EasyReduceParams(
  command: String = null,
  trimCommandOutput: Boolean = false,
  imageName: String = "ubuntu:14.04",
  inputPath: String = null,
  outputPath: String = null,
  fifoReadTimeout: Int = RunUtils.FIFO_READ_TIMEOUT,
  wholeFiles: Boolean = false,
  local: Boolean = false,
  dockerSudo: Boolean = false)

object EasyReduce extends Logging {

  def run(params: EasyReduceParams) = {

    //Start Spark context
    val conf = new SparkConf()
      .setAppName(s"Reduce: ${params.command}")
    if (params.local) {
      conf.setMaster("local[2]")
      conf.set("spark.default.parallelism","2")
    }
    val sc = new SparkContext(conf)

    //Read input data
    val defaultParallelism = 
      sc.getConf.get("spark.default.parallelism").toInt
    val data = if (params.wholeFiles) {
      sc.wholeTextFiles(
          params.inputPath, 
          defaultParallelism)
        .map(_._2) //remove file name
    } else {
      sc.textFile(params.inputPath, defaultParallelism)
    }

    //Reduce data
    val result = data.reduce {
      case (record1, record2) =>
        //Init RunUtils
        val threadPool = RunUtils.createThreadPool
        val run = new RunUtils(threadPool)
        //Make fifos
        val inputFifo1 = run.mkfifo("input1")
        val inputFifo2 = run.mkfifo("input2")
        val outputFifo = run.mkfifo("output")
        //Write record to fifo
        run.writeToFifo(inputFifo1, record1)
        run.writeToFifo(inputFifo2, record2)
        //Run command in container
        val t0 = System.currentTimeMillis
        val dockerOpts = s"-v ${inputFifo1.getAbsolutePath}:/input1 " +
          s"-v ${inputFifo2.getAbsolutePath}:/input2 " +
          s"-v ${outputFifo.getAbsolutePath}:/output"
        run.dockerRun(params.command, 
            params.imageName, 
            dockerOpts,
            params.dockerSudo)
        //Read result from fifo
        val results = run.readFromFifo(outputFifo, params.fifoReadTimeout)
        val dockerTime = System.currentTimeMillis - t0
        //Log serial time
        logInfo(s"Docker ran in (millisec.): $dockerTime")
        //Delete the fifos
        inputFifo1.delete
        inputFifo2.delete
        outputFifo.delete
        //Shut down thread pool
        threadPool.shutdown()
        //Trim results and return
        if (params.trimCommandOutput) {
          results.trim
        } else {
          results
        }
    }

    //Save restult
    sc.parallelize(result.lines.toSeq, 1)
      .saveAsTextFile(params.outputPath)

    //Stop Spark context
    sc.stop

  }

  def main(args: Array[String]) {

    val defaultParams = EasyReduceParams()

    val parser = new OptionParser[EasyReduceParams]("EasyReduce") {
      head("EasyReduce: reduce a distributed dataset using a command from a Docker container.")
      opt[String]("imageName")
        .text("Docker image name (default: \"ubuntu:14.04\").")
        .action((x, c) => c.copy(imageName = x))
      opt[String]("command")
        .required
        .text("command to run inside the Docker container, " + 
            "e.g. 'expr sum $(cat /input1) + $(cat /input2) > /output'. " +
            "The command needs to be associative and commutative.")
        .action((x, c) => c.copy(command = x))
      opt[Unit]("trimCommandOutput")
        .text("if set the command output will get trimmed.")
        .action((_, c) => c.copy(trimCommandOutput = true))
      opt[Unit]("wholeFiles")
        .text("if set, multiple input files will be loaded from an input directory. The command will " +
              "executed in parallel, on the whole files. In contrast, when this is not set "+
              "the file/files in input is/are splitted line by line, and the command is executed in parallel "+
              "on each line of the file.")
        .action((_, c) => c.copy(wholeFiles = true))
      opt[Int]("commandTimeout")
        .text(s"execution timeout for the command, in sec. (default: ${RunUtils.FIFO_READ_TIMEOUT}).")
        .action((x, c) => c.copy(fifoReadTimeout = x))
      opt[Unit]("local")
        .text("set to run in local mode (useful for testing purpose).")
        .action((_, c) => c.copy(local = true))
      opt[Unit]("dockerSudo")
        .text("set to run docker with passwordless sudo.")
        .action((_, c) => c.copy(dockerSudo = true))
      arg[String]("inputPath")
        .required
        .text("dataset input path. Must be a directory if wholeFiles is set.")
        .action((x, c) => c.copy(inputPath = x))
      arg[String]("outputPath")
        .required
        .text("result output path.")
        .action((x, c) => c.copy(outputPath = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }

  }

}