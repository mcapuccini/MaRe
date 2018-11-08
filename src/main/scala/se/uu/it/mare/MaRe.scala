package se.uu.it.mare

import java.io.File
import java.util.UUID

import scala.reflect.ClassTag
import scala.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

/**
 * 	MaRe API.
 *
 * @constructor
 * @param rdd input RDD
 */
class MaRe[T: ClassTag](val rdd: RDD[T]) extends Serializable {

  @transient protected lazy val log = Logger.getLogger(getClass.getName)

  /**
   * Returns the number of partitions of the underlying RDD.
   *
   * @return number of partitions of the underlying RDD
   */
  def getNumPartitions: Int = rdd.getNumPartitions

  /**
   * Caches the underlying RDD in memory.
   *
   * @return new MaRe object
   */
  def cache: MaRe[T] = {
    new MaRe(rdd.cache)
  }

  /**
   * Repartitions the underlying RDD to the specified number of partitions.
   *
   * @param numPartitions number of partitions for the underlying RDD
   * @return new MaRe object
   */
  def repartition(numPartitions: Int): MaRe[T] = {
    new MaRe(rdd.repartition(numPartitions))
  }

  /**
   * Maps each RDD partition through a Docker container command.
   *
   * @param inputMountPoint mount point for the partitions that is passed to the containers
   * @param outputMountPoint mount point where the processed partition is read back to Spark
   * @param imageName Docker image name
   * @param command Docker command
   * @param forcePull if set to true the Docker image will be pulled even if present locally
   * @return new MaRe object
   */
  def map[U: ClassTag](
    inputMountPoint:  MountPoint[T],
    outputMountPoint: MountPoint[U],
    imageName:        String,
    command:          String,
    forcePull:        Boolean       = false): MaRe[U] = {
    val resRDD = rdd.mapPartitions { partition =>

      // Create temporary files
      val tmpDir = new File(Properties.envOrElse("TMPDIR", "/tmp"))
      val tmpIn = new File(tmpDir, "mare_" + UUID.randomUUID.toString)
      val tmpOut = new File(tmpDir, "mare_" + UUID.randomUUID.toString)
      inputMountPoint.writePartitionToHostPath(partition, tmpIn)
      outputMountPoint.createEmptyMountPoint(tmpOut)
      FileUtils.forceDeleteOnExit(tmpIn)
      FileUtils.forceDeleteOnExit(tmpOut)

      // Run docker
      DockerHelper.run(
        imageName,
        command,
        bindFiles = Seq(tmpIn, tmpOut),
        volumeFiles = Seq(new File(inputMountPoint.path), new File(outputMountPoint.path)),
        forcePull)

      // Retrieve output
      val output = outputMountPoint.readPartitionFromHostPath(tmpOut)

      // Remove temporary files
      FileUtils.forceDelete(tmpIn)
      FileUtils.forceDelete(tmpOut)

      // Return output
      output

    }
    new MaRe(resRDD)
  }

  /**
   * Reduces the data to a single partition using a Docker container command. The command is applied
   * using a tree reduce strategy.
   *
   * @param inputMountPoint mount point for the partitions that is passed to the containers
   * @param outputMountPoint mount point where the processed partition is read back to Spark
   * @param imageName Docker image name
   * @param command Docker command
   * @param depth depth of the reduce tree (default: 2, must be greater than or equal to 1)
   * @param forcePull if set to true the Docker image will be pulled even if present locally
   * @return new MaRe object
   *
   */
  def reduce(
    inputMountPoint:  MountPoint[T],
    outputMountPoint: MountPoint[T],
    imageName:        String,
    command:          String,
    depth:            Int           = 2,
    forcePull:        Boolean       = false): MaRe[T] = {
    require(depth >= 2, s"Depth must be greater than or equal to 2 but got $depth.")

    // First apply command within partition (to reduce size before repartitioning)
    val reduced = this.map(
      inputMountPoint,
      outputMountPoint,
      imageName,
      command,
      forcePull)

    val scale = math.max(math.ceil(math.pow(this.getNumPartitions, 1.0 / depth)).toInt, 2)
    if (depth > 2 && this.getNumPartitions > this.getNumPartitions / scale) {
      // If depth greater than 2 and partitions will scale down
      reduced.repartition(reduced.getNumPartitions / scale)
        .reduce(
          inputMountPoint,
          outputMountPoint,
          imageName,
          command,
          depth - 1)
    } else if (reduced.getNumPartitions > 1) {
      // If there is more than 1 partition
      reduced.repartition(1).map(
        inputMountPoint,
        outputMountPoint,
        imageName,
        command,
        forcePull)
    } else {
      reduced
    }
  }

}