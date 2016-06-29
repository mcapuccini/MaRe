package se.uu.farmbio.easymr

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class ReduceParams(
    command: String = null,
    imageName: String = "ubuntu:14.04",
    inputData: String = null,
    outputData: String = null,
    wholeFiles: Boolean = false,
    local: Boolean = false)

object Reduce {
  
  /*def run(params: ReduceParams) {
    
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
      .replaceAll("<input:1>", "/inputFifo1")
      .replaceAll("<input:2>", "/inputFifo2")
      .replaceAll("<output>", "/outputFifo")

    //Reduce data
    data.reduce { (record1, record2) =>
      val inputFifo1 = RunUtils.mkfifo("input1")
      val inputFifo2 = RunUtils.mkfifo("input2")
      val outputFifo = RunUtils.mkfifo("output")
      val dockerOpts = s"-v ${inputFifo1.getAbsolutePath}:/inputFifo1 " +
        s"-v ${inputFifo2.getAbsolutePath}:/inputFifo2 " +
        s"-v ${outputFifo.getAbsolutePath}:/outputFifo"
      RunUtils.writeToFifo(inputFifo1, record1)
      RunUtils.writeToFifo(inputFifo2, record2)
      RunUtils.dockerRun(toRun, params.imageName, dockerOpts)
      val results = RunUtils.readFromFifo(outputFifo)
      inputFifo1.delete
      inputFifo2.delete
      outputFifo.delete
      results
    }
    
    //Stop Spark context
    sc.stop

  }*/


}