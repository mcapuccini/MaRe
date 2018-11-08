package se.uu.it.mare

import scala.util.Properties

import org.junit.runner.RunWith
import org.scalatest.FunSuite

import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.core.command.PullImageResultCallback
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DockerHelperTest extends FunSuite {

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
  private val dockerClient = DockerClientBuilder.getInstance(config).build

  test("Map-like Docker run, image not present") {

    // Remove image if present
    val localImgList = dockerClient.listImagesCmd
      .withImageNameFilter("busybox:1")
      .exec
    if (localImgList.size > 0) {
      dockerClient.removeImageCmd("busybox:1")
        .withForce(true)
        .exec
    }

    // Run docker
    val statusCode = DockerHelper.run(
      imageName = "busybox:1",
      command = "true",
      bindFiles = Seq(),
      volumeFiles = Seq(),
      forcePull = false)

    assert(statusCode == 0)

  }

  test("Map-like Docker run, image present") {

    // Pull image
    dockerClient.pullImageCmd("busybox:1")
      .exec(new PullImageResultCallback)
      .awaitSuccess()

    // Run docker
    val statusCode = DockerHelper.run(
      imageName = "busybox:1",
      command = "true",
      bindFiles = Seq(),
      volumeFiles = Seq(),
      forcePull = false)

    assert(statusCode == 0)

  }

  test("Map-like Docker run, force pull") {

    // Pull image
    dockerClient.pullImageCmd("busybox:1")
      .exec(new PullImageResultCallback)
      .awaitSuccess()

    // Run docker
    val statusCode = DockerHelper.run(
      imageName = "busybox:1",
      command = "true",
      bindFiles = Seq(),
      volumeFiles = Seq(),
      forcePull = true)

    assert(statusCode == 0)

  }

}
