package se.uu.it.mare

import java.io.File
import java.util.regex.Pattern

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

private[mare] object MaRe {

  // Logger
  private lazy val log = Logger.getLogger(getClass.getName)

  def mapLambda(
    imageName:        String,
    command:          String,
    inputMountPoint:  String,
    outputMountPoint: String,
    records:          Iterator[String],
    recordDelimiter:  String) = {

    // Create temporary files
    val inputFile = FileHelper.writeToTmpFile(records, recordDelimiter)
    val outputFile = FileHelper.createTmpFile

    // Run docker
    val docker = new DockerHelper
    docker.run(
      imageName,
      command,
      bindFiles = Seq(inputFile, outputFile),
      volumeFiles = Seq(new File(inputMountPoint), new File(outputMountPoint)))

    // Retrieve output
    val output = FileHelper.readFromFile(outputFile, recordDelimiter)

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
 * MaRe leverages the power of Docker and Spark to run and scale your serial tools
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
 *  @param reduceInputMountPoint1 reduce mount point for the first input file that is passed to the containers
 *  @param reduceInputMountPoint2 recude mount point for the second input file that is passed to the containers
 */
class MaRe(
  private val rdd:      RDD[String],
  val inputMountPoint:  String      = "/input",
  val outputMountPoint: String      = "/output") extends Serializable {

  // Logger
  @transient private lazy val log = Logger.getLogger(getClass.getName)

  val recordDelimiter =
    Option(rdd.sparkContext.hadoopConfiguration.get("textinputformat.record.delimiter"))
      .getOrElse("\n")

  /**
   * It returns the underlying RDD for this MaRe object.
   */
  def getRDD = rdd

  /**
   * It caches the underlying RDD in memory
   */
  def cache = {
    new MaRe(
      rdd.cache,
      inputMountPoint,
      outputMountPoint)
  }

  /**
   * It repartitions the underlying RDD to the specified number of partitions.
   *
   * @param numPartitions number of partitions to use in the underlying RDD
   */
  def repartition(numPartitions: Int) = {
    new MaRe(
      rdd.repartition(numPartitions),
      inputMountPoint,
      outputMountPoint)
  }

  /**
   * Returns the number of partitions of the underlying RDD.
   */
  def getNumPartitions = rdd.getNumPartitions

  /**
   * It sets the mount point for the input chunk that is passed to the containers.
   *
   * @param inputMountPoint mount point for the input chunk that is passed to the containers
   */
  def setInputMountPoint(inputMountPoint: String) = {
    new MaRe(
      rdd,
      inputMountPoint,
      outputMountPoint)
  }

  /**
   * It sets the mount point where the processed data is read back to Spark.
   *
   * @param outputMountPoint mount point where the processed data is read back to Spark
   */
  def setOutputMountPoint(outputMountPoint: String) = {
    new MaRe(
      rdd,
      inputMountPoint,
      outputMountPoint)
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
    command:   String) = {

    // Map partitions to avoid opening too many files
    val resRDD = rdd.mapPartitions { records =>
      MaRe.mapLambda(
        imageName, command,
        inputMountPoint, outputMountPoint,
        records, recordDelimiter)
    }
    new MaRe(
      resRDD,
      inputMountPoint,
      outputMountPoint)

  }

  /**
   * It reduces a RDD to a single String using a Docker container command. The command is applied
   * using a tree reduce strategy. Data is mounted to the specified inputMountPoint and read back 
   * from the specified outputMountPoint.
   *
   * @param imageName a Docker image name available in each node
   * @param command a command to run in the Docker container, this should read from
   * inputMountPoint and write back to outputMountPoint, and it should perform an
   * associative and commutative operation (for the parallelization to work)
   * @param depth depth of the reduce tree (default: 2, must be greater than or equal to 1)
   *
   */
  def reduce(
    imageName: String,
    command:   String,
    depth:     Int    = 2): String = {

    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")

    val scale = math.max(math.ceil(math.pow(this.getNumPartitions, 1 / depth)).toInt, 2)
    if (depth > 2 && this.getNumPartitions > this.getNumPartitions / scale) {
      // If depth greater than 2 and partitions will scale down, map and repartition
      val reduced = this.map(imageName, command)
      reduced.repartition(reduced.getNumPartitions / scale)
        .reduce(imageName, command, depth - 1)
    } else if (this.getNumPartitions > 1) {
      // If depth is in {1,2} map, then collect and reduce locally
      val reduced = this.map(imageName, command)
      val records = reduced.getRDD.collect
      MaRe.mapLambda(
        imageName, command,
        inputMountPoint, outputMountPoint,
        records.iterator, recordDelimiter)
        .map(_ + recordDelimiter)
        .mkString
    } else {
      // If there is only 1 partition it's better to reduce before to collect
      val reduced = this.map(imageName, command)
      val records = reduced.getRDD.collect
      records.map(_ + recordDelimiter).mkString
    }

  }

}
