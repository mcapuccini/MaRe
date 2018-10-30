package se.uu.it.mare

import java.io.File

import scala.io.Source
import scala.util.Properties

import org.junit.runner.RunWith
import org.scalatest.FunSuite

import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.core.command.PullImageResultCallback
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DockerHelperTest extends FunSuite {

  // Test image
  private val imageName = "ubuntu:xenial"
  private val imageNameNoTag = "ubuntu"

  // Init Docker client
  private val configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder()
  if (Properties.envOrNone("DOCKER_HOST") != None) {
    configBuilder.withDockerHost(System.getenv("DOCKER_HOST"))
  }
  if (Properties.envOrNone("DOCKER_TLS_VERIFY") != None) {
    val tlsVerify = System.getenv("DOCKER_TLS_VERIFY") == "1"
    configBuilder.withDockerTlsVerify(tlsVerify)
  }
  if (Properties.envOrNone("DOCKER_CERT_PATH") != None) {
    configBuilder.withDockerCertPath(System.getenv("DOCKER_CERT_PATH"))
  }
  private val config = configBuilder.build
  val dockerClient = DockerClientBuilder.getInstance(config).build

  test("Map-like Docker run, image not present") {

    // Remove image if present
    val localImgList = dockerClient.listImagesCmd
      .withImageNameFilter(imageName)
      .exec
    if (localImgList.size > 0) {
      dockerClient.removeImageCmd(imageName)
        .withForce(true)
        .exec
    }

    // Create temporary files
    val inputFile = FileHelper.writeToTmpFile(
      Seq("hello world").iterator, recordDelimiter = "\n")
    val outputFile = FileHelper.createTmpFile

    // Run docker
    DockerHelper.run(
      imageName,
      command = "cat /input > /output",
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(new File("/input"), new File("/output")),
      forcePull = false)

    val content = Source.fromFile(outputFile).mkString
    assert(content == "hello world\n")

  }

  test("Map-like Docker run, image present") {

    // Pull image
    dockerClient.pullImageCmd(imageName)
      .exec(new PullImageResultCallback)
      .awaitSuccess()

    // Create temporary files
    val inputFile = FileHelper.writeToTmpFile(
      Seq("hello world").iterator, recordDelimiter = "\n")
    val outputFile = FileHelper.createTmpFile

    // Run docker
    DockerHelper.run(
      imageName,
      command = "cat /input > /output",
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(new File("/input"), new File("/output")),
      forcePull = false)

    // Test results
    val content = Source.fromFile(outputFile).mkString
    assert(content == "hello world\n")
    
  }
  
  test("Map-like Docker run, no tag") {

    // Create temporary files
    val inputFile = FileHelper.writeToTmpFile(
      Seq("hello world").iterator, recordDelimiter = "\n")
    val outputFile = FileHelper.createTmpFile

    // Run docker
    DockerHelper.run(
      imageNameNoTag,
      command = "cat /input > /output",
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(new File("/input"), new File("/output")),
      forcePull = false)

    // Test results
    val content = Source.fromFile(outputFile).mkString
    assert(content == "hello world\n")
    
  }

}
