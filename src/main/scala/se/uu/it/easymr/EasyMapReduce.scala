package se.uu.it.easymr

import java.io.File
import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.spark.util.LongAccumulator

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

  def reduceLambda(
    imageName: String,
    command: String,
    reduceInputMountPoint1: String,
    reduceInputMountPoint2: String,
    outputMountPoint: String,
    record1: String,
    record2: String,
    recordDelimiter: String) = {

    // Create temporary files
    val inputFile1 = EasyFiles.writeToTmpFile(Seq(record1).iterator, recordDelimiter)
    val inputFile2 = EasyFiles.writeToTmpFile(Seq(record2).iterator, recordDelimiter)
    val outputFile = EasyFiles.createTmpFile

    // Run docker
    val docker = new EasyDocker
    docker.run(
      imageName,
      command,
      bindFiles = Seq(inputFile1, inputFile2, outputFile),
      volumeFiles = Seq(
        new File(reduceInputMountPoint1),
        new File(reduceInputMountPoint2),
        new File(outputMountPoint)))

    // Retrieve output
    val output = EasyFiles.readFromFile(outputFile, recordDelimiter)

    // Remove temporary files
    log.info(s"Deleteing temporary file: ${inputFile1.getAbsolutePath}")
    inputFile1.delete
    log.info(s"Temporary file '${inputFile1.getAbsolutePath}' deleted successfully")
    log.info(s"Deleteing temporary file: ${inputFile2.getAbsolutePath}")
    inputFile2.delete
    log.info(s"Temporary file '${inputFile2.getAbsolutePath}' deleted successfully")
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
 *  @param reduceInputMountPoint1 reduce mount point for the first input file that is passed to the containers
 *  @param reduceInputMountPoint2 recude mount point for the second input file that is passed to the containers
 */
class EasyMapReduce(
    private val rdd: RDD[String],
    val inputMountPoint: String = "/input",
    val outputMountPoint: String = "/output",
    val reduceInputMountPoint1: String = "/input1",
    val reduceInputMountPoint2: String = "/input2") extends Serializable {

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
    new EasyMapReduce(
      rdd,
      inputMountPoint,
      outputMountPoint,
      reduceInputMountPoint1,
      reduceInputMountPoint2)
  }

  /**
   * It sets the mount point where the processed data is read back to Spark.
   *
   * @param outputMountPoint mount point where the processed data is read back to Spark
   */
  def setOutputMountPoint(outputMountPoint: String) = {
    new EasyMapReduce(
      rdd,
      inputMountPoint,
      outputMountPoint,
      reduceInputMountPoint1,
      reduceInputMountPoint2)
  }

  /**
   * It sets the mount point for the first input file that is passed to the containers in the reduce method.
   *
   * @param reduceInputMountPoint1 mount point for the first input file that is passed to the containers
   * in the reduce method
   */
  def setReduceInputMountPoint1(reduceInputMountPoint1: String) = {
    new EasyMapReduce(
      rdd,
      inputMountPoint,
      outputMountPoint,
      reduceInputMountPoint1,
      reduceInputMountPoint2)
  }

  /**
   * It sets the mount point for the second input file that is passed to the containers in the reduce method.
   *
   * @param reduceInputMountPoint2 mount point for the second input file that is passed to the containers
   * in the reduce method
   */
  def setReduceInputMountPoint2(reduceInputMountPoint2: String) = {
    new EasyMapReduce(
      rdd,
      inputMountPoint,
      outputMountPoint,
      reduceInputMountPoint1,
      reduceInputMountPoint2)
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
  def mapPartitions(
    imageName: String,
    command: String) = {

    // Map partitions to avoid opening too many files
    val resRDD = rdd.mapPartitions { records =>
      EasyMapReduce.mapLambda(
        imageName, command,
        inputMountPoint, outputMountPoint,
        records, recordDelimiter)
    }
    new EasyMapReduce(resRDD,
      inputMountPoint,
      outputMountPoint,
      reduceInputMountPoint1,
      reduceInputMountPoint2)

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
  def reducePartitions(
    imageName: String,
    command: String) = {

    // First reduce within partitions
    val reducedPartitions = this.mapPartitions(imageName, command).getRDD

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

  /**
   * It maps each RDD record through a Docker container command.
   * Data is mounted to the specified inputMountPoint and read back
   * from the specified outputMountPoint. If the container command
   * returns multiple records, results are flattened.
   *
   * @param imageName a Docker image name available in each node
   * @param command a command to run in the Docker container, this should read from
   * inputMountPoint and write back to outputMountPoint
   */
  def map(
    imageName: String,
    command: String) = {

    val resRDD = rdd.flatMap { record =>
      val resIt = EasyMapReduce.mapLambda(
        imageName, command,
        inputMountPoint, outputMountPoint,
        Seq(record).iterator, recordDelimiter)
      resIt
    }
    new EasyMapReduce(resRDD,
      inputMountPoint,
      outputMountPoint,
      reduceInputMountPoint1,
      reduceInputMountPoint2)

  }

  /**
   * It reduces a RDD to a single String using a Docker container command.
   * The command is applied to couples of RDD records. Data is mounted to the specified
   * reduceInputMountPoint1 and reduceInputMountPoint2, and it is read back from
   * the specified outputMountPoint.
   *
   * @param imageName a Docker image name available in each node
   * @param command a command to run in the Docker container, this should read from
   * reduceInputMountPoint1 and reduceInputMountPoint2, and it should write back to
   * outputMountPoint. The command should perform an associative and commutative operation
   * (for the parallelization to work).
   *
   */
  def reduce(
    imageName: String,
    command: String) = {

    // Reduce
    rdd.reduce {
      case (r1, r2) =>
        EasyMapReduce.reduceLambda(
          imageName, command,
          reduceInputMountPoint1,
          reduceInputMountPoint2,
          outputMountPoint,
          r1, r2, recordDelimiter)
          .next
    }

  }

}
