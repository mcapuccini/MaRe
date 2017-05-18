package se.uu.it.easymr

import java.io.File

import scala.io.Source

import org.apache.spark.rdd.RDD

private[easymr] object EasyMapReduce {

  final val MAP_INPUT = new File("/input")
  final val MAP_OUTPUT = new File("/output")
  
  def mapLambda(
    imageName: String,
    command: String,
    record: String): String = {
    EasyMapReduce
      .mapLambda(
        imageName,
        command,
        Seq(record).iterator)
      .mkString
  }

  def mapLambda(
    imageName: String,
    command: String,
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
      volumeFiles = Seq(MAP_INPUT, MAP_OUTPUT))

    //Retrieve output
    val output = Source.fromFile(outputFile).getLines

    //Remove temporary files
    inputFile.delete
    outputFile.delete

    //Return output
    output

  }

}

class EasyMapReduce(private val rdd: RDD[String]) {

  def getRDD = rdd

  def map(
    imageName: String,
    command: String) = {

    //Map partitions to avoid opening too many files
    val resRDD = rdd.mapPartitions(EasyMapReduce.mapLambda(imageName, command, _))
    new EasyMapReduce(resRDD)

  }

  def reduce(
    imageName: String,
    command: String) = {

    //First reduce within partitions
    val reducedPartitions = this.map(imageName, command).getRDD

    //Reduce
    reducedPartitions.reduce { case(rp1,rp2) =>
      EasyMapReduce.mapLambda(imageName, command, rp1+"\n"+rp2)
    }

  }

}