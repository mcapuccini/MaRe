package se.uu.farmbio.easymr

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

case class EasyReduceParams(
  command: String = null,
  trimComandOutput: Boolean = true,
  imageName: String = "ubuntu:14.04",
  inputPath: String = null,
  outputPath: String = null,
  fifoReadTimeout: Int = RunUtils.FIFO_READ_TIMEOUT,
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
    val data = sc.textFile(params.inputPath)

    //Format command
    val toRun = params.command
      .replaceAll("<input:1>", "/inputFifo1")
      .replaceAll("<input:2>", "/inputFifo2")
      .replaceAll("<output>", "/outputFifo")
      
    //Reduce data
    val result = data.reduce { case(record1,record2) =>
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
      results.trim
    }
    
    //Save restult
    sc.parallelize(result.lines.toSeq, 1)
      .saveAsTextFile(params.outputPath)
    
    //Stop Spark context
    sc.stop  

  }

}