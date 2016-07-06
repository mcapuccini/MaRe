package se.uu.farmbio.easymr

import java.util.concurrent.Executors

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import scopt.OptionParser

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()
  override def generateFileNameForKeyValue(
    key: Any,
    value: Any,
    name: String): String =
    key.asInstanceOf[String]
}

case class EasyMapParams(
  command: String = null,
  trimComandOutput: Boolean = true,
  imageName: String = "ubuntu:14.04",
  inputPath: String = null,
  outputPath: String = null,
  fifoReadTimeout: Int = RunUtils.FIFO_READ_TIMEOUT,
  wholeFiles: Boolean = false,
  local: Boolean = false)

object EasyMap {

  def run(params: EasyMapParams) = {

    //Start Spark context
    val conf = new SparkConf()
      .setAppName(s"Map: ${params.command}")
    if (params.local) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)

    //Read input data
    val data = readInputData(
      sc,
      params.inputPath,
      params.outputPath,
      params.wholeFiles)

    //Format command
    val toRun = params.command
      .replaceAll("<input>", "/inputFifo")
      .replaceAll("<output>", "/outputFifo")

    //Map data
    val result = data.map {
      case (index, record) =>
        //Init RunUtils
        val threadPool = Executors.newFixedThreadPool(10)
        val run = new RunUtils(threadPool)
        //Make fifos
        val inputFifo = run.mkfifo("input")
        val outputFifo = run.mkfifo("output")
        //Write record to fifo
        run.writeToFifo(inputFifo, record)
        //Run command in container
        val dockerOpts = s"-v ${inputFifo.getAbsolutePath}:/inputFifo " +
          s"-v ${outputFifo.getAbsolutePath}:/outputFifo"
        run.dockerRun(toRun, params.imageName, dockerOpts)
        //Read result from fifo
        val results = run.readFromFifo(outputFifo, params.fifoReadTimeout)
        //Delete the fifos
        inputFifo.delete
        outputFifo.delete
        //Shut down thread pool
        threadPool.shutdown()
        //Trim results and return
        if (params.trimComandOutput) {
          (index, results.trim)
        } else {
          (index, results)
        }
    }

    //Save results
    if (params.wholeFiles) { //Save with multiple output
      val numFiles = result.map(_._1).distinct.count
      result.partitionBy(new HashPartitioner(numFiles.toInt))
        .saveAsHadoopFile(params.outputPath,
          classOf[String],
          classOf[String],
          classOf[RDDMultipleTextOutputFormat])
    } else { //Save as single file
      result.map(_._2) //remove index
        .saveAsTextFile(params.outputPath)
    }

    //Stop Spark context
    sc.stop

  }

  private def readInputData(
    sc: SparkContext,
    inputPath: String,
    outputPath: String,
    wholeFiles: Boolean) = {
    if (wholeFiles) {
      //Get output extension
      val outExt = FilenameUtils.getExtension(outputPath)
      //Load files
      sc.wholeTextFiles(inputPath)
        .map {
          case (filename, content) =>
            //Trim extension and path
            val noExt = FilenameUtils.removeExtension(filename)
            val trimmedName = FilenameUtils.getBaseName(noExt)
            //Set trimmed name and index, with output extension
            if (outExt != null && outExt.length > 0) {
              (s"${trimmedName}.${outExt}", content)
            } else {
              (trimmedName, content)
            }
        }
    } else {
      sc.textFile(inputPath)
        .map((inputPath, _))
    }
  }

  def main(args: Array[String]) {

    val defaultParams = EasyMapParams()

    val parser = new OptionParser[EasyMapParams]("Easy Map") {
      head("EasyMap: map a distributed dataset using a command form a Docker container.")
      opt[String]("imageName")
        .text("Docker image name (default: \"ubuntu:14.04\").")
        .action((x, c) => c.copy(imageName = x))
      opt[String]("command")
        .required
        .text("command to run inside the Docker container, e.g. rev <input> > <output>.")
        .action((x, c) => c.copy(command = x))
      opt[Unit]("noTrim")
        .text("if set the command output will not get trimmed.")
        .action((_, c) => c.copy(trimComandOutput = false))
      opt[Unit]("wholeFiles")
        .text("if set, multiple input files will be loaded from an input directory. The command will " +
          "executed in parallel, on the whole files. In contrast, when this is not set " +
          "the file/files in input is/are splitted line by line, and the command is executed in parallel " +
          "on each line of the file.")
        .action((_, c) => c.copy(wholeFiles = true))
      opt[Int]("commandTimeout")
        .text(s"execution timeout for the command, in sec. (default: ${RunUtils.FIFO_READ_TIMEOUT}).")
        .action((x, c) => c.copy(fifoReadTimeout = x))
      opt[Unit]("local")
        .text("set to run in local mode (useful for testing purpose).")
        .action((_, c) => c.copy(local = true))
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