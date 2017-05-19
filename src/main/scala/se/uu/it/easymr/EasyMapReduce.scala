package se.uu.it.easymr

import java.io.File

import scala.io.Source

import org.apache.spark.rdd.RDD

private[easymr] object EasyMapReduce {

  def mapLambda(
    imageName: String,
    command: String,
    inputFilePath: String,
    outputFilePath: String,
    record: String): String = {
    EasyMapReduce
      .mapLambda(
        imageName,
        command,
        inputFilePath,
        outputFilePath,
        Seq(record).iterator)
      .mkString
  }

  def mapLambda(
    imageName: String,
    command: String,
    inputFilePath: String,
    outputFilePath: String,
    records: Iterator[String]) = {

    //Create temporary files
    val inputFile = EasyFiles.writeToTmpFile(records)
    val outputFile = EasyFiles.createTmpFile

    //Run docker
    val docker = new EasyDocker
    docker.run(
      imageName,
      command,
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(new File(inputFilePath), new File(outputFilePath)))

    //Retrieve output
    val output = Source.fromFile(outputFile).getLines

    //Remove temporary files
    inputFile.delete
    outputFile.delete
    
    //Return output
    output

  }

}

class EasyMapReduce(
    private val rdd: RDD[String],
    val inputFilePath: String = "/input",
    val outputFilePath: String = "/output") extends Serializable {

  def getRDD = rdd
  
  def setInputPath(inputFilePath: String) = {
    new EasyMapReduce(rdd, inputFilePath, outputFilePath)
  }
  
  def setOutputPath(outputFilePath: String) = {
    new EasyMapReduce(rdd, inputFilePath, outputFilePath)
  }

  def map(
    imageName: String,
    command: String) = {

    //Map partitions to avoid opening too many files
    val resRDD = rdd.mapPartitions(
      EasyMapReduce.mapLambda(imageName, command, inputFilePath, outputFilePath, _))
    new EasyMapReduce(resRDD, inputFilePath, outputFilePath)

  }

  def reduce(
    imageName: String,
    command: String) = {

    //First reduce within partitions
    val reducedPartitions = this.map(imageName, command).getRDD

    //Reduce
    reducedPartitions.reduce {
      case (rp1, rp2) =>
        EasyMapReduce.mapLambda(
          imageName, command, inputFilePath, outputFilePath, rp1 + "\n" + rp2)
    }

  }

}