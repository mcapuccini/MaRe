package se.uu.farmbio.easymr

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scopt.OptionParser

case class EasyReduceParams(
  command: String = null,
  trimComandOutput: Boolean = true,
  imageName: String = "ubuntu:14.04",
  inputPath: String = null,
  outputPath: String = null,
  fifoReadTimeout: Int = RunUtils.FIFO_READ_TIMEOUT,
  wholeFiles: Boolean = false,
  local: Boolean = false)

object EasyReduce {

  def run(params: EasyReduceParams) = {

    //Start Spark context
    val conf = new SparkConf()
      .setAppName(s"Reduce: ${params.command}")
    if (params.local) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)

    //Read input data
    val data = if (params.wholeFiles) {
      sc.wholeTextFiles(params.inputPath)
        .map(_._2) //remove file name
    } else {
      sc.textFile(params.inputPath)
    }

    //Format command
    val toRun = params.command
      .replaceAll("<input:1>", "/inputFifo1")
      .replaceAll("<input:2>", "/inputFifo2")
      .replaceAll("<output>", "/outputFifo")

    //Reduce data
    val result = data.reduce {
      case (record1, record2) =>
        //Make fifos
        val inputFifo1 = RunUtils.mkfifo("input1")
        val inputFifo2 = RunUtils.mkfifo("input2")
        val outputFifo = RunUtils.mkfifo("output")
        //Write record to fifo
        RunUtils.writeToFifo(inputFifo1, record1)
        RunUtils.writeToFifo(inputFifo2, record2)
        //Run command in container
        val dockerOpts = s"-v ${inputFifo1.getAbsolutePath}:/inputFifo1 " +
          s"-v ${inputFifo2.getAbsolutePath}:/inputFifo2 " +
          s"-v ${outputFifo.getAbsolutePath}:/outputFifo"
        RunUtils.dockerRun(toRun, params.imageName, dockerOpts)
        //Read result from fifo
        val results = RunUtils.readFromFifo(outputFifo, params.fifoReadTimeout)
        //Delete the fifos
        inputFifo1.delete
        inputFifo2.delete
        outputFifo.delete
        //Trim results and return
        if (params.trimComandOutput) {
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
      opt[String]("command")
        .required
        .text("command to run inside the Docker container, " +
          "e.g. sum $(cat <input:1>) + $(cat <input:2>) > <output>.")
        .action((x, c) => c.copy(command = x))
      opt[Unit]("noTrim")
        .text("if set the command output will not get trimmed.")
        .action((_, c) => c.copy(trimComandOutput = false))
      opt[String]("imageName")
        .text("Docker image name (default: \"ubuntu:14.04\")")
        .action((x, c) => c.copy(imageName = x))
      opt[String]("inputPath")
        .required
        .text("Dataset input path. Must be a directory if wholeFiles is set.")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("outputPath")
        .required
        .text("Result output path")
        .action((x, c) => c.copy(outputPath = x))
      opt[Int]("commandTimeout")
        .text(s"execution timeout for the command, in sec. (default: ${RunUtils.FIFO_READ_TIMEOUT})")
        .action((x, c) => c.copy(fifoReadTimeout = x))
      opt[Unit]("wholeFiles")
        .text("if set, multiple input files will be loaded from an input directory. The command will " +
          "executed in parallel, on the whole files. In contrast, when this is not set " +
          "the file/files in input is/are splitted line by line, and the command is executed in parallel " +
          "on each line of the file")
        .action((_, c) => c.copy(wholeFiles = true))
      opt[Unit]("local")
        .text("set to run in local mode (useful for testing purpose)")
        .action((_, c) => c.copy(local = true))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }

  }

}