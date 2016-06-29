package se.uu.farmbio.easymr

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class MapParams(
    command: String = null,
    imageName: String = "ubuntu:14.04",
    inputData: String = null,
    outputData: String = null,
    wholeFiles: Boolean = false,
    fifoReadTimeout: Int = 1200,
    local: Boolean = false)

object Map {
  
  def run(params: MapParams) = {

    //Start Spark context
    val conf = new SparkConf()
      .setAppName(s"Reduce: ${params.command}")
    if (params.local) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    
    //Read input data
    val data = if(params.wholeFiles) {
      sc.wholeTextFiles(params.inputData)
        .map(_._2)
    } else {
      sc.textFile(params.inputData)
    }
    
    //Format command
    val toRun = params.command
      .replaceAll("<input>", "/inputFifo/input")
      .replaceAll("<output>", "/outputFifo/output")

    //Map data
    val result = data.map { record =>
      val inputFifo = RunUtils.mkfifo("input")
      val outputFifo = RunUtils.mkfifo("output")
      val dockerOpts = s"-v ${inputFifo.getParent}:/inputFifo " +
        s"-v ${outputFifo.getParent}:/outputFifo"
      RunUtils.writeToFifo(inputFifo, record)
      RunUtils.dockerRun(toRun, params.imageName, dockerOpts)
      val results = RunUtils.readFromFifo(outputFifo, params.fifoReadTimeout)
      inputFifo.delete
      outputFifo.delete
      results
    }
    
    result.saveAsTextFile(params.outputData)
    
    //Stop Spark context
    sc.stop

  }

}