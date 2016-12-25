package se.uu.it.easymr

import java.io.File

import scala.io.Source

import org.apache.spark.rdd.RDD

private[easymr] object EasyMapReduce {

  final val MAP_INPUT = new File("/input")
  final val MAP_OUTPUT = new File("/output")

  final val REDUCE_INPUT1 = new File("/input1")
  final val REDUCE_INPUT2 = new File("/input2")
  final val REDUCE_OUTPUT = new File("/output")

  def mapLambda(
    imageName: String,
    command: String,
    record: String) = {

    //Create temporary files
    val inputFile = EasyFiles.writeToTmpFile(record)
    val outputFile = EasyFiles.createTmpFile

    //Run docker
    val docker = new EasyDocker
    docker.run(
      imageName,
      command,
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(MAP_INPUT, MAP_OUTPUT))

    //Retrieve output
    val output = Source.fromFile(outputFile).mkString

    //Remove temporary files
    inputFile.delete
    outputFile.delete

    //Return output
    output

  }

  def mapWholeFiles(
    rdd: RDD[(String, String)],
    imageName: String,
    command: String) = {

    //Map
    rdd.map {
      case (filename, content) =>
        (filename, mapLambda(imageName, command, content))
    }

  }

}

class EasyMapReduce(private val rdd: RDD[String]) {

  def getRDD = rdd

  def map(
    imageName: String,
    command: String) = {

    //Map
    val resRDD = rdd.map(EasyMapReduce.mapLambda(imageName, command, _))
    new EasyMapReduce(resRDD)

  }

  def mapPartitions(
    imageName: String,
    command: String) = {

    //Map partitions
    val resRDD = rdd.mapPartitions { it =>
      val partition = it.reduce(_ + "\n" + _)
      val res = EasyMapReduce.mapLambda(imageName, command, partition)
      Source.fromString(res).getLines
    }
    new EasyMapReduce(resRDD)

  }

  def reduce(
    imageName: String,
    command: String) = {

    //Reduce
    rdd.reduce {
      case (record1, record2) =>

        //Create temporary files
        val inputFile1 = EasyFiles.writeToTmpFile(record1)
        val inputFile2 = EasyFiles.writeToTmpFile(record2)
        val outputFile = EasyFiles.createTmpFile

        //Run docker
        val docker = new EasyDocker
        docker.run(
          imageName,
          command,
          bindFiles = Seq(inputFile1, inputFile2, outputFile),
          volumeFiles = Seq(
            EasyMapReduce.REDUCE_INPUT1,
            EasyMapReduce.REDUCE_INPUT2,
            EasyMapReduce.REDUCE_OUTPUT))

        //Retrieve output
        val output = Source.fromFile(outputFile).mkString

        //Remove temporary files
        inputFile1.delete()
        inputFile2.delete()
        outputFile.delete()

        //Return output
        output

    }

  }

}