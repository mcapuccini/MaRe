package se.uu.it.easymr

import java.io.File

import scala.collection.JavaConversions.seqAsJavaList
import scala.util.Properties

import org.apache.log4j.Logger

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.Frame
import com.github.dockerjava.api.model.Volume
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.core.command.AttachContainerResultCallback
import com.github.dockerjava.core.command.WaitContainerResultCallback

private class EasyDocker extends Serializable {

  // Init client
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

  // Logging
  @transient private lazy val log = Logger.getLogger(getClass.getName)
  private class LoggingCallback extends AttachContainerResultCallback {
    override def onNext(item: Frame) = {
      log.info(item)
      super.onNext(item)
    }
  }

  def run(
    imageName: String,
    command: String,
    bindFiles: Seq[File],
    volumeFiles: Seq[File]) = {
    
    // Init client
    val dockerClient = DockerClientBuilder.getInstance(config).build

    // Create volumes and binds
    def volumes = volumeFiles.map { file =>
      val volumePath = file.getAbsolutePath
      new Volume(volumePath)
    }
    def binds = bindFiles.zip(volumes).map {
      case (file, volume) =>
        val bindPath = file.getAbsolutePath
        new Bind(bindPath, volume)
    }

    // Run container
    val container = dockerClient.createContainerCmd(imageName)
      .withEntrypoint("sh", "-c")
      .withCmd(command)
      .withVolumes(volumes)
      .withBinds(binds)
      .exec
    log.info(s"Running container '${container.getId}' (image: '$imageName', command: '$command'")
    val t0 = System.currentTimeMillis()
    val exec = dockerClient.startContainerCmd(container.getId).exec

    // Attach container output to log4j
    dockerClient
      .attachContainerCmd(container.getId)
      .withStdErr(true)
      .withStdOut(true)
      .withLogs(true)
      .exec(new LoggingCallback)
      .awaitCompletion

    // Wait for container exit code
    log.info(s"Waiting for container ${container.getId}")
    val statusCode = dockerClient.waitContainerCmd(container.getId())
      .exec(new WaitContainerResultCallback())
      .awaitStatusCode()
    val t1 = System.currentTimeMillis()
    log.info(s"Container ${container.getId} took ${t1-t0} ms")

    // Raise exception if statusCode != 0
    if (statusCode != 0) {
      throw new RuntimeException(
        s"Container ${container.getId} exited with non zero exit code: $statusCode")
    }
    log.info(s"Container ${container.getId} exited with zero exit code: 0")
    
    // Close Docker client
    dockerClient.close
    
  }

}
