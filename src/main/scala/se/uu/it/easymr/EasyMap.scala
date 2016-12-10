package se.uu.it.easymr

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import scopt.OptionParser
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger


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
  trimCommandOutput: Boolean = false,
  imageName: String = "ubuntu:14.04",
  inputPath: String = null,
  outputPath: String = null,
  fifoReadTimeout: Int = RunUtils.FIFO_READ_TIMEOUT,
  wholeFiles: Boolean = false,
  local: Boolean = false,
  dockerSudo: Boolean = false,
  dockerOpts: String = "")

object EasyMap {
  
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def run(params: EasyMapParams) = {

    //Start Spark context
    val conf = new SparkConf()
      .setAppName(s"Map: ${params.command}")
    if (params.local) {
      conf.setMaster("local[2]")
      conf.set("spark.default.parallelism", "2")
    }
    val sc = new SparkContext(conf)

    //Read input data
    val data = readInputData(
      sc,
      params.inputPath,
      params.outputPath,
      params.wholeFiles)

    //Map data
    val result = data.map {
      case (index, record) =>
        //Init RunUtils
        val threadPool = RunUtils.createThreadPool
        val run = new RunUtils(threadPool)
        //Make fifos
        val inputFifo = run.mkfifo("input")
        val outputFifo = run.mkfifo("output")
        //Write record to fifo
        run.writeToFifo(inputFifo, record)
        //Run command in container
        val t0 = System.currentTimeMillis
        val dockerOpts = s"-v ${inputFifo.getAbsolutePath}:/input " +
          s"-v ${outputFifo.getAbsolutePath}:/output" +
          s" ${params.dockerOpts}" //additional user options
        run.dockerRun(params.command,
          params.imageName,
          dockerOpts.trim,
          params.dockerSudo)
        //Read result from fifo
        val results = run.readFromFifo(outputFifo, params.fifoReadTimeout)
        val dockerTime = System.currentTimeMillis - t0
        //Log serial time
        log.info(s"Docker ran in (millisec.): $dockerTime")
        //Delete the fifos
        inputFifo.delete
        outputFifo.delete
        //Shut down thread pool
        threadPool.shutdown()
        //Trim results and return
        if (params.trimCommandOutput) {
          (index, results.trim)
        } else {
          (index, results)
        }
    }

    //Save results
    if (params.wholeFiles) { //Save with multiple output
      //Count files to output
      val it = FileSystem
        .get(sc.hadoopConfiguration)
        .listFiles(new Path(params.inputPath), false)
      var numFiles = 0 // Can't go functional on this :-(
      while(it.hasNext()) {
        it.next
        numFiles+=1
      }
      //Save on separated files
      result.partitionBy(new HashPartitioner(numFiles))
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
    val defaultParallelism =
      sc.getConf.get("spark.default.parallelism").toInt
    if (wholeFiles) {
      //Get output extension
      val outExt = FilenameUtils.getExtension(outputPath)
      //Load files
      sc.wholeTextFiles(
        inputPath,
        defaultParallelism)
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
      sc.textFile(inputPath,
        sc.getConf.get("spark.default.parallelism").toInt)
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
        .text("command to run inside the Docker container, e.g. 'rev /input > /output'.")
        .action((x, c) => c.copy(command = x))
      opt[Unit]("trimCommandOutput")
        .text("if set the command output will get trimmed.")
        .action((_, c) => c.copy(trimCommandOutput = true))
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
      opt[Unit]("dockerSudo")
        .text("set to run docker with passwordless sudo.")
        .action((_, c) => c.copy(dockerSudo = true))
      opt[String]("dockerOpts")
        .text("additional options for \"Docker run\" (default: none).")
        .action((x, c) => c.copy(dockerOpts = x))
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