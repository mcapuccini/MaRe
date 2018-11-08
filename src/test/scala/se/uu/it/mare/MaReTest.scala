package se.uu.it.mare

import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

private object MaReTest {

  def sparkCountGC(rdd: RDD[String]): String = {
    rdd.map(_.count(c => c == 'g' || c == 'c').toString)
      .reduce {
        case (lineCount1, lineCount2) =>
          (lineCount1.toInt + lineCount2.toInt).toString
      }
  }

}

@RunWith(classOf[JUnitRunner])
class MaReTest extends FunSuite with SharedSparkContext {

  private val partitions = 5

  test("GC count DNA string") {

    val testPath = getClass.getResource("dna/fasta/dna_1.fasta").getPath
    val rdd = sc.textFile(testPath, partitions)

    (2 to 4).foreach { depth => // Try a few reduce depths

      val res = new MaRe(rdd)
        .map(
          inputMountPoint = TextFile("/dna"),
          outputMountPoint = TextFile("/count"),
          imageName = "busybox:1",
          command = "grep -o '[gc]' /dna | wc -l > /count")
        .reduce(
          inputMountPoint = TextFile("/counts"),
          outputMountPoint = TextFile("/sum"),
          imageName = "busybox:1",
          command = "awk '{s+=$1} END {print s}' /counts > /sum",
          depth)
        .rdd.collect()

      // There should be a single element
      assert(res.length == 1)

      // Check if results matches with the one computed with the standard RDD API
      val dna = sc.textFile(testPath)
      val toMatch = MaReTest.sparkCountGC(dna)
      assert(res(0) == toMatch)

    }

  }

  test("GC count DNA files") {

    val testPath = getClass.getResource("dna/fasta").getPath
    val rdd = sc.wholeTextFiles(testPath, partitions)

    val res = new MaRe(rdd)
      .map(
        inputMountPoint = WholeTextFiles("/dna"),
        outputMountPoint = WholeTextFiles("/counts"),
        imageName = "busybox:1",
        command =
          """
          for filename in /dna/dna_*.fasta; do
            grep -o '[gc]' $filename | wc -l > /counts/$(basename $filename).sum
          done
          """)
      .reduce(
        inputMountPoint = WholeTextFiles("/counts"),
        outputMountPoint = WholeTextFiles("/sum"),
        imageName = "busybox:1",
        command = "awk '{s+=$1} END {print s}' /counts/*.sum > /sum/${RANDOM}.sum")
      .rdd.collect()

    // There should be a single element
    assert(res.length == 1)

    // Check if results matches with the one computed with the standard RDD API
    val dna = sc.textFile(testPath)
    val toMatch = MaReTest.sparkCountGC(dna)
    assert(res(0)._2 == toMatch + "\n")

  }

  test("GC count gzipped DNA files") {

    val testPath = getClass.getResource("dna/zipped").getPath
    val rdd = sc.binaryFiles(testPath, partitions)
      .map { case (path, data) => (path, data.toArray) }

    val res = new MaRe(rdd)
      .map(
        inputMountPoint = BinaryFiles("/dna"),
        outputMountPoint = BinaryFiles("/counts"),
        imageName = "busybox:1",
        command =
          """
          for filename in /dna/dna_*.fasta.gz; do
            gunzip -c $filename | grep -o '[gc]' | wc -l > /counts/$(basename $filename).sum
            gzip /counts/$(basename $filename).sum
          done
          """)
      .reduce(
        inputMountPoint = BinaryFiles("/counts"),
        outputMountPoint = BinaryFiles("/sum"),
        imageName = "busybox:1",
        command =
          """
          gunzip /counts/*.sum.gz
          outfile=${RANDOM}.sum
          awk '{s+=$1} END {print s}' /counts/*.sum > /sum/${outfile}.sum
          gzip /sum/${outfile}.sum
          """)
      .map(
        inputMountPoint = BinaryFiles("/sum"),
        outputMountPoint = TextFile("/unzipped"),
        imageName = "busybox:1",
        command =
          """
          gunzip -c /sum/*.sum.gz > /unzipped
          """)
      .rdd.collect()

    // There should be a single element
    assert(res.length == 1)

    // Check if results matches with the one computed with the standard RDD API
    val dna = sc.textFile(testPath)
    val toMatch = MaReTest.sparkCountGC(dna)
    assert(res(0) == toMatch)

  }

  test("GC count with file types switch") {

    val testPath = getClass.getResource("dna/zipped").getPath
    val rdd = sc.binaryFiles(testPath, partitions)
      .map { case (path, data) => (path, data.toArray) }

    val res = new MaRe(rdd)
      .map(
        inputMountPoint = BinaryFiles("/zipped"),
        outputMountPoint = WholeTextFiles("/unzipped"),
        imageName = "busybox:1",
        command =
          """
          for filename in /zipped/*; do
            fasta_out=$(basename "${filename}" .gz)
            gunzip -c $filename > /unzipped/$fasta_out
          done
          """)
      .map(
        inputMountPoint = WholeTextFiles("/dna"),
        outputMountPoint = TextFile("/count"),
        imageName = "busybox:1",
        command = "grep -o '[gc]' dna/* | wc -l > /count")
      .reduce(
        inputMountPoint = TextFile("/counts"),
        outputMountPoint = TextFile("/sum"),
        imageName = "busybox:1",
        command = "awk '{s+=$1} END {print s}' /counts > /sum")
      .rdd.collect()

    // There should be a single element
    assert(res.length == 1)

    // Check if results matches with the one computed with the standard RDD API
    val dna = sc.textFile(testPath)
    val toMatch = MaReTest.sparkCountGC(dna)
    assert(res(0) == toMatch)

  }

}
