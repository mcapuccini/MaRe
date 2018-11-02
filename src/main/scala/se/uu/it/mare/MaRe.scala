package se.uu.it.mare

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

/**
 * MaRe leverages the power of Docker and Spark to run and scale your serial tools
 * in MapReduce fashion. The data goes from Spark through the Docker container, and back to Spark
 * after being processed, via Unix files. Please make sure that the TMPDIR environment variable
 * in the worker nodes points to a tmpfs to reduce overhead when running in production. To make sure
 * that the TMPDIR is properly set in each node you can use the "setExecutorEnv" method from the
 * SparkConf class when initializing the SparkContext.
 */
object MaRe {
  
  /**
   * Init MaRe from a String RDD. Use this data type if you need to process a large text file.
   * 
   * @param rdd input RDD
   */
  def apply(rdd: RDD[String]) = new TextFileMaRe(rdd)
  
}

/**	
 * 	Abstract MaRe API for any data type.
 * 
 *  @constructor
 *  @param rdd input RDD
 */
abstract class MaRe[DataType <: Any](
  private val rdd: RDD[DataType]
) extends Serializable {
  
  /**
   * Returns the underlying RDD for this MaRe object.
   */
  def getRDD = rdd
  
  /**
   * Returns the number of partitions of the underlying RDD.
   */
  def getNumPartitions = rdd.getNumPartitions

  /**
   * Caches the underlying RDD in memory
   */
  def cache: MaRe[DataType]

  /**
   * Repartitions the underlying RDD to the specified number of partitions.
   *
   * @param numPartitions number of partitions to use in the underlying RDD
   */
  def repartition(numPartitions: Int): MaRe[DataType]

  /**
   * Sets the mount point for the input chunk that is passed to the containers.
   *
   * @param inputMountPoint mount point for the input chunk that is passed to the containers
   */
  def setInputMountPoint(inputMountPoint: String): MaRe[DataType]

  /**
   * Sets the mount point where the processed data is read back to Spark.
   *
   * @param outputMountPoint mount point where the processed data is read back to Spark
   */
  def setOutputMountPoint(outputMountPoint: String): MaRe[DataType]

  /**
   * If set to true it will pull the Docker image even if present locally.
   *
   * @param forcePull set to true to force Docker image pulling
   */
  def forcePull(forcePull: Boolean): MaRe[DataType]

  /**
   * Maps each RDD partition through a Docker container command.
   * Data is mounted to the specified inputMountPoint and read back
   * from the specified outputMountPoint.
   *
   * @param imageName a Docker image name available in each node
   * @param command a command to run in the Docker container, this should read from
   * inputMountPoint and write back to outputMountPoint
   */
  def map(
    imageName: String,
    command:   String): MaRe[DataType]

  /**
   * Reduces a RDD to a single String using a Docker container command. The command is applied
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
    depth:     Int    = 2): String
  
}