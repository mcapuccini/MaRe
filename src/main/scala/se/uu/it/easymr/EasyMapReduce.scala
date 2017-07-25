package se.uu.it.easymr

import java.io.File
import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger

private[easymr] object EasyMapReduce {
  
  // Logger
  private lazy val log = Logger.getLogger(getClass.getName)

  def mapLambda(
    imageName: String,
    command: String,
    inputMountPoint: String,
    outputMountPoint: String,
    records: Iterator[String],
    recordDelimiter: String) = {

    // Create temporary files
    val inputFile = EasyFiles.writeToTmpFile(records, recordDelimiter)
    val outputFile = EasyFiles.createTmpFile

    // Run docker
    val docker = new EasyDocker
    docker.run(
      imageName,
      command,
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(new File(inputMountPoint), new File(outputMountPoint)))

    // Retrieve output
    val output = EasyFiles.readFromFile(outputFile, recordDelimiter)

    // Remove temporary files
    log.info(s"Deleteing temporary file: ${inputFile.getAbsolutePath}")
    inputFile.delete
    log.info(s"Temporary file '${inputFile.getAbsolutePath}' deleted successfully")
    log.info(s"Deleteing temporary file: ${outputFile.getAbsolutePath}")
    outputFile.delete
    log.info(s"Temporary file '${outputFile.getAbsolutePath}' deleted successfully")

    // Return output
    output

  }

}

/**
 * EasyMapReduce leverages the power of Docker and Spark to run and scale your serial tools
 * in MapReduce fashion. The data goes from Spark through the Docker container, and back to Spark
 * after being processed, via Unix files. Please make sure that the TMPDIR environment variable
 * in the worker nodes points to a tmpfs to reduce overhead when running in production. To make sure
 * that the TMPDIR is properly set in each node you can use the "setExecutorEnv" method from the
 * SparkConf class when initializing the SparkContext.
 *
 *  @constructor
 *  @param rdd input RDD
 *  @param inputMountPoint mount point for the input chunk that is passed to the containers
 *  @param outputMountPoint mount point where the processed data is read back to Spark
 */
class EasyMapReduce(
    private val rdd: RDD[String],
    val inputMountPoint: String = "/input",
    val outputMountPoint: String = "/output") extends Serializable {
  
  // Logger
  @transient private lazy val log = Logger.getLogger(getClass.getName)

  val recordDelimiter =
    Option(rdd.sparkContext.hadoopConfiguration.get("textinputformat.record.delimiter"))
      .getOrElse("\n")

  /**
   * It returns the underlying RDD for this EasyMapReduce object.
   */
  def getRDD = rdd

  /**
   * It sets the mount point for the input chunk that is passed to the containers.
   *
   * @param inputMountPoint mount point for the input chunk that is passed to the containers
   */
  def setInputMountPoint(inputMountPoint: String) = {
    new EasyMapReduce(rdd, inputMountPoint, outputMountPoint)
  }

  /**
   * It sets the mount point where the processed data is read back to Spark.
   *
   * @param outputMountPoint mount point where the processed data is read back to Spark
   */
  def setOutputMountPoint(outputMountPoint: String) = {
    new EasyMapReduce(rdd, inputMountPoint, outputMountPoint)
  }

  /**
   * It maps each RDD partition through a Docker container command.
   * Data is mounted to the specified inputMountPoint and read back
   * from the specified outputMountPoint.
   *
   * @param imageName a Docker image name available in each node
   * @param command a command to run in the Docker container, this should read from
   * inputMountPoint and write back to outputMountPoint
   */
  def map(
    imageName: String,
    command: String) = {

    // Map partitions to avoid opening too many files
    val resRDD = rdd.mapPartitions { records =>
      EasyMapReduce.mapLambda(
        imageName, command,
        inputMountPoint, outputMountPoint,
        records, recordDelimiter)
    }
    new EasyMapReduce(resRDD, inputMountPoint, outputMountPoint)

  }

  /**
   * It reduces a RDD to a single String using a Docker container command. The command is applied first
   * to each RDD partition, and then to couples of RDD records. Data is mounted to the specified
   * inputMountPoint and read back from the specified outputMountPoint.
   *
   * @param imageName a Docker image name available in each node
   * @param command a command to run in the Docker container, this should read from
   * inputMountPoint and write back to outputMountPoint, and it should perform an
   * associative and commutative operation (for the parallelization to work)
   *
   */
  def reduce(
    imageName: String,
    command: String) = {

    // First reduce within partitions
    val reducedPartitions = this.map(imageName, command).getRDD

    // Reduce
    reducedPartitions.reduce {
      case (rp1, rp2) =>
        log.info(s"Splitting records by record delimiter: $recordDelimiter")
        val delimiterRegex = Pattern.quote(recordDelimiter)
        val records = rp1.split(delimiterRegex) ++ rp2.split(delimiterRegex)
        log.info(s"Records sucessfully splitted by record delimiter: $recordDelimiter")
        EasyMapReduce.mapLambda(
          imageName, command,
          inputMountPoint, outputMountPoint,
          records.iterator, recordDelimiter)
          .map(_ + recordDelimiter)
          .mkString
    }

  }

}
