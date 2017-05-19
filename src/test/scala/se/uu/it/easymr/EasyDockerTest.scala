package se.uu.it.easymr

import java.io.File

import scala.io.Source

import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EasyDockerTest extends FunSuite {

  test("Map-like Docker run") {

    //Create temporary files
    val inputFile = EasyFiles.writeToTmpFile(Seq("hello world").iterator)
    val outputFile = EasyFiles.createTmpFile

    //Run docker
    val docker = new EasyDocker
    docker.run(
      imageName = "ubuntu:xenial", // assumes ubuntu:xenial was pulled 
      command = "cat /input > /output",
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(new File("/input"), new File("/output")))

    val content = Source.fromFile(outputFile).mkString
    assert(content == "hello world\n")

  }

}