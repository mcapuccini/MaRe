package se.uu.it.mare

import org.apache.spark.SharedSparkContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.apache.spark.rdd.RDD

private object DNAUtils {
  def sparkCountGC(rdd: RDD[String]) = {
    rdd.map(_.count(c => c == 'g' || c == 'c').toString)
      .reduce {
        case (lineCount1, lineCount2) =>
          (lineCount1.toInt + lineCount2.toInt).toString
      }
  }
}

@RunWith(classOf[JUnitRunner])
class GcCountTest extends FunSuite with SharedSparkContext {

  private val partitions = 5

  test("GC count in DNA string, defaults") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath, partitions)

    val res = MaRe(rdd)
      .map(
        imageName = "ubuntu:xenial",
        command = "grep -o '[gc]' /input | wc -l > /output")
      .reduce(
        imageName = "ubuntu:xenial",
        command = "awk '{s+=$1} END {print s}' /input > /output")

    // Check if results matches with the one computed with the standard RDD API
    val dna = sc.textFile(getClass.getResource("dna/dna.txt").getPath)
    val toMatch = DNAUtils.sparkCountGC(dna)
    assert(res == toMatch + "\n")

  }

  test("GC count in DNA string, set volume files") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath, partitions)

    val res = MaRe(rdd)
      .setInputMountPoint("/input.dna")
      .setOutputMountPoint("/output.dna")
      .map(
        imageName = "ubuntu:xenial",
        command = "grep -o '[gc]' /input.dna | wc -l > /output.dna")
      .reduce(
        imageName = "ubuntu:xenial",
        command = "awk '{s+=$1} END {print s}' /input.dna > /output.dna")

    // Check if results matches with the one computed with the standard RDD API
    val dna = sc.textFile(getClass.getResource("dna/dna.txt").getPath)
    val toMatch = DNAUtils.sparkCountGC(dna)
    assert(res == toMatch + "\n")

  }

  test("GC count in DNA string, depth 3") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath, partitions)

    val res = MaRe(rdd)
      .setInputMountPoint("/input.dna")
      .setOutputMountPoint("/output.dna")
      .map(
        imageName = "ubuntu:xenial",
        command = "grep -o '[gc]' /input.dna | wc -l > /output.dna")
      .reduce(
        imageName = "ubuntu:xenial",
        command = "awk '{s+=$1} END {print s}' /input.dna > /output.dna",
        depth = 3)

    // Check if results matches with the one computed with the standard RDD API
    val dna = sc.textFile(getClass.getResource("dna/dna.txt").getPath)
    val toMatch = DNAUtils.sparkCountGC(dna)
    assert(res == toMatch + "\n")

  }

  test("GC count in DNA string, depth 1") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath, partitions)

    val res = MaRe(rdd)
      .setInputMountPoint("/input.dna")
      .setOutputMountPoint("/output.dna")
      .map(
        imageName = "ubuntu:xenial",
        command = "grep -o '[gc]' /input.dna | wc -l > /output.dna")
      .reduce(
        imageName = "ubuntu:xenial",
        command = "awk '{s+=$1} END {print s}' /input.dna > /output.dna",
        depth = 1)

    // Check if results matches with the one computed with the standard RDD API
    val dna = sc.textFile(getClass.getResource("dna/dna.txt").getPath)
    val toMatch = DNAUtils.sparkCountGC(dna)
    assert(res == toMatch + "\n")

  }

}
