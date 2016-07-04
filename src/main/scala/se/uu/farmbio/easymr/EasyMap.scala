package se.uu.farmbio.easymr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import java.io.File
import org.apache.commons.io.FilenameUtils

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
        //Make fifos
        val inputFifo = RunUtils.mkfifo("input")
        val outputFifo = RunUtils.mkfifo("output")
        //Write record to fifo
        RunUtils.writeToFifo(inputFifo, record)
        //Run command in container
        val dockerOpts = s"-v ${inputFifo.getAbsolutePath}:/inputFifo " +
          s"-v ${outputFifo.getAbsolutePath}:/outputFifo"
        RunUtils.dockerRun(toRun, params.imageName, dockerOpts)
        //Read result from fifo
        val results = RunUtils.readFromFifo(outputFifo, params.fifoReadTimeout)
        //Delete the fifos
        inputFifo.delete
        outputFifo.delete
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

  private[easymr] def readInputData(
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
            (s"${trimmedName}.${outExt}", content)
        }
    } else {
      sc.textFile(inputPath)
        .map((inputPath, _))
    }
  }

}