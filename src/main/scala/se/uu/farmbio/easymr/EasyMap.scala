package se.uu.farmbio.easymr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class EasyMapParams(
    command: String = null,
    trimComandOutput: Boolean = true,
    imageName: String = "ubuntu:14.04",
    inputPath: String = null,
    outputPath: String = null,
    fifoReadTimeout: Int = RunUtils.FIFO_READ_TIMEOUT,
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
    val data = sc.textFile(params.inputPath)
    
    //Format command
    val toRun = params.command
      .replaceAll("<input>", "/inputFifo")
      .replaceAll("<output>", "/outputFifo")

    //Map data
    val result = data.map { record =>
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
      if(params.trimComandOutput) {
        results.trim
      } else {
        results 
      }
    }
    
    result.saveAsTextFile(params.outputPath)
    
    //Stop Spark context
    sc.stop

  }

}