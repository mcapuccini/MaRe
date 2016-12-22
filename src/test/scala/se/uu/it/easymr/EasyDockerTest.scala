package se.uu.it.easymr

import java.io.File

import scala.io.Source

import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite

class EasyDockerTest
    extends FunSuite
    with SharedSparkContext {

  test("Map-like docker run") {

    //Create temporary files
    val inputFile = EasyFiles.writeToTmpFile("hello world")
    val outputFile = EasyFiles.createTmpFile

    //Run docker
    val docker = new EasyDocker
    docker.run(
      imageName = "busybox", // assumes busybox was pulled 
      command = "cat /input > /output",
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(new File("/input"), new File("/output")))

    val content = Source.fromFile(outputFile).mkString
    assert(content == "hello world")

  }

  test("Reduce-like docker run") {

    //Create temporary files
    val inputFile1 = EasyFiles.writeToTmpFile("hello ")
    val inputFile2 = EasyFiles.writeToTmpFile("world")
    val outputFile = EasyFiles.createTmpFile

    //Run docker
    val docker = new EasyDocker
    docker.run(
      imageName = "busybox", // assumes busybox was pulled 
      command = "cat /input1 > /output && cat /input2 >> /output",
      bindFiles = Seq(inputFile1, inputFile2, outputFile),
      volumeFiles = Seq(
        new File("/input1"),
        new File("/input2"),
        new File("/output")))

    val content = Source.fromFile(outputFile).mkString
    assert(content == "hello world")

  }

}