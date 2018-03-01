package se.uu.it.mare

import java.io.File

import scala.io.Source

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DockerHelperTest extends FunSuite {

  test("Map-like Docker run") {

    // Create temporary files
    val inputFile = FileHelper.writeToTmpFile(
      Seq("hello world").iterator, recordDelimiter = "\n")
    val outputFile = FileHelper.createTmpFile

    // Run docker
    val docker = new DockerHelper
    docker.run(
      imageName = "ubuntu:xenial", // assumes ubuntu:xenial was pulled
      command = "cat /input > /output",
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(new File("/input"), new File("/output")))

    val content = Source.fromFile(outputFile).mkString
    assert(content == "hello world\n")

  }

}
