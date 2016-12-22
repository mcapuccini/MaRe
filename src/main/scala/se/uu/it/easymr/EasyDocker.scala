package se.uu.it.easymr

import java.io.File

import scala.collection.JavaConversions.seqAsJavaList

import org.apache.log4j.Logger

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.Frame
import com.github.dockerjava.api.model.Volume
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.core.command.AttachContainerResultCallback
import com.github.dockerjava.core.command.WaitContainerResultCallback

private[easymr] class EasyDocker extends Serializable {

  // Init client
  private val configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder()
  if (System.getenv("DOCKER_HOST") != null) {
    configBuilder.withDockerHost(System.getenv("DOCKER_HOST"))
  }
  if (System.getenv("DOCKER_TLS_VERIFY") != null) {
    val tlsVerify = System.getenv("DOCKER_TLS_VERIFY") == "1"
    configBuilder.withDockerTlsVerify(tlsVerify)
  }
  if (System.getenv("DOCKER_CERT_PATH") != null) {
    configBuilder.withDockerCertPath(System.getenv("DOCKER_CERT_PATH"))
  }
  private val config = configBuilder.build
  private val dockerClient = DockerClientBuilder.getInstance(config).build

  private class LoggingCallback extends AttachContainerResultCallback {
    @transient lazy val log = Logger.getLogger(getClass.getName)
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

    //Create volumes and binds
    def volumes = volumeFiles.map { file =>
      val volumePath = file.getAbsolutePath
      new Volume(volumePath)
    }
    def binds = bindFiles.zip(volumes).map {
      case(file, volume) => 
        val bindPath = file.getAbsolutePath
        new Bind(bindPath,volume)
    }

    //Run container
    val container = dockerClient.createContainerCmd(imageName)
      .withEntrypoint("sh", "-c")
      .withCmd(command)
      .withVolumes(volumes)
      .withBinds(binds)
      .exec
    val exec = dockerClient.startContainerCmd(container.getId).exec

    //Attach container output to log4j
    dockerClient
      .attachContainerCmd(container.getId)
      .withStdErr(true)
      .withStdOut(true)
      .withLogs(true)
      .exec(new LoggingCallback)
      .awaitCompletion

    //Wait for container exit code  
    val statusCode = dockerClient.waitContainerCmd(container.getId())
      .exec(new WaitContainerResultCallback())
      .awaitStatusCode()

    //Remove container
    dockerClient.removeContainerCmd(container.getId).exec

    //Raise exception if statusCode != 0
    if (statusCode != 0) {
      throw new RuntimeException(
        s"container exited with non zero exit code: $statusCode")
    }

  }

}