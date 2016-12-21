package se.uu.it.easymr

import org.apache.spark.rdd.RDD

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.Volume
import com.github.dockerjava.core.DockerClientBuilder
import java.io.File
import scala.io.Source

object EasyMapReduce {

  private final val MAP_INPUT = new File("/input")
  private final val MAP_OUTPUT = new File("/output")

  private final val REDUCE_INPUT1 = new File("/input1")
  private final val REDUCE_INPUT2 = new File("/input2")
  private final val REDUCE_OUTPUT = new File("/output")

  private def mapLambda( 
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
    Source.fromFile(outputFile).mkString

  }

  def map(
    rdd: RDD[String],
    imageName: String,
    command: String) = {

    //Map
    rdd.map(mapLambda(imageName, command, _))

  }

  private[easymr] def mapWholeFiles(
    rdd: RDD[(String, String)],
    imageName: String,
    command: String) = {

    //Map
    rdd.map { case(filename, content) =>
      (filename, mapLambda(imageName, command, content))
    }

  }

  def reduce(
    rdd: RDD[String],
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
            REDUCE_INPUT1,
            REDUCE_INPUT2,
            REDUCE_OUTPUT))

        //Retrieve output
        Source.fromFile(outputFile).mkString

    }

  }

}