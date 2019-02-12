package se.uu.it.mare

import java.io.File
import java.util.UUID

import scala.reflect.ClassTag
import scala.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.Partitioner
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner

/**
 * MaRe API.
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
    inputMountPoint: MountPoint[T],
    outputMountPoint: MountPoint[U],
    imageName: String,
    command: String,
    forcePull: Boolean = false): MaRe[U] = {
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
   * @param depth depth of the reduce tree (default: 2, must be greater than or equal to 2)
   * @param forcePull if set to true the Docker image will be pulled even if present locally
   * @return new MaRe object
   *
   */
  def reduce(
    inputMountPoint: MountPoint[T],
    outputMountPoint: MountPoint[T],
    imageName: String,
    command: String,
    depth: Int = 2,
    forcePull: Boolean = false): MaRe[T] = {
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

  /**
   * Repartitions data according to keyBy and a custom partitioner.
   *
   * @param keyBy given a record computes a key
   * @param partitioner custom partitioner
   */
  def repartitionBy(
    keyBy: T => Any,
    partitioner: Partitioner): MaRe[T] = {
    val partRDD = rdd.keyBy(keyBy).partitionBy(partitioner)
    new MaRe(partRDD.map(_._2))
  }

  /**
   * Repartitions data according to keyBy and org.apache.spark.HashPartitioner.
   *
   * @param keyBy given a record computes a key
   * @param numPartitions number of partitions for the resulting RDD
   */
  def repartitionBy(
    keyBy: T => Any,
    numPartitions: Int): MaRe[T] = {
    this.repartitionBy(keyBy, new HashPartitioner(numPartitions))
  }

  /**
   * :: Experimental ::
   * First collects the data locally on disk, and then reduces and writes it to a local output path
   * using a Docker container command. This is an experimental feature (use at your own risk).
   *
   * @param inputMountPoint mount point for the partitions that is passed to the containers
   * @param outputMountPoint mount point where the processed partition is read back to Spark
   * @param imageName Docker image name
   * @param command Docker command
   * @param localOutPath local output path
   * @param forcePull if set to true the Docker image will be pulled even if present locally
   * @param intermediateStorageLevel intermediate results storage level (default: MEMORY_AND_DISK)
   *
   */
  @Experimental
  def collectReduce(
    inputMountPoint: MountPoint[T],
    outputMountPoint: MountPoint[T],
    imageName: String,
    command: String,
    localOutPath: String,
    forcePull: Boolean = false,
    intermediateStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): Unit = {

    // Force computation of previous stages (barrier execution may be used instead in the future)
    val sc = rdd.sparkContext
    val indices = rdd.partitions.indices
    val persistedRDD = rdd.persist(intermediateStorageLevel)
    sc.runJob(rdd, (iter: Iterator[T]) => None, indices)

    // Create temporary directory
    val tmpDirParent = new File(localOutPath).getParent
    val tmpDir = new File(tmpDirParent, ".temporary_" + UUID.randomUUID.toString)
    tmpDir.mkdirs
    FileUtils.forceDeleteOnExit(tmpDir)
    // Create temporary input file
    val tmpIn = new File(tmpDir, "mare_" + UUID.randomUUID.toString)
    inputMountPoint.createEmptyMountPoint(tmpIn)
    FileUtils.forceDeleteOnExit(tmpIn)
    // Create output file
    val outPath = new File(localOutPath)
    outputMountPoint.createEmptyMountPoint(outPath)

    // Write all partitions in the temporary input directory
    persistedRDD.partitions.indices.iterator.foreach { i =>
      val p = sc.runJob(rdd, (iter: Iterator[T]) => iter.toArray, Seq(i)).head
      inputMountPoint.appendPartitionToHostPath(p.iterator, tmpIn)
    }

    // Run Docker
    DockerHelper.run(
      imageName,
      command,
      bindFiles = Seq(tmpIn, outPath),
      volumeFiles = Seq(new File(inputMountPoint.path), new File(outputMountPoint.path)),
      forcePull)

    // Remove temporary files
    FileUtils.forceDelete(tmpIn)
    FileUtils.forceDelete(tmpDir)

  }

}
