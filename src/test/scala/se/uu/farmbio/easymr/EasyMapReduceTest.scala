package se.uu.farmbio.easymr

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.google.common.io.Files
import java.io.PrintWriter
import org.scalatest.BeforeAndAfterAll
import scala.reflect.io.Path
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import java.io.File

@RunWith(classOf[JUnitRunner])
class EasyMapReduceTest extends FunSuite with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setAppName("MapReduceTest")
    .setMaster("local[*]")

  val tempDir = Files.createTempDir

  test("easy map") {

    val params = EasyMapParams(
      command = "rev /input > /output",
      imageName = "ubuntu:14.04",
      local = true,
      inputPath = getClass.getResource("dna/dna.txt").getPath,
      outputPath = tempDir.getAbsolutePath + "/rev.txt",
      fifoReadTimeout = 30)
    EasyMap.run(params)

    val reverseTest = Source.fromFile(getClass.getResource("dna/dna.txt").getPath)
      .getLines.map(_.reverse)

    val sc = new SparkContext(conf)
    val reverseOut = sc.textFile(tempDir.getAbsolutePath + "/rev.txt").collect
    sc.stop

    assert(reverseTest.toSet == reverseOut.toSet)

  }

  test("easy reduce") {

    //Make a test input from DNA
    val lineCount = Source.fromFile(
      getClass.getResource("dna/dna.txt").getPath)
      .getLines
      .map(_.filter(n => n == 'g' || n == 'c').length)
    new PrintWriter(tempDir.getAbsolutePath + "/count_by_line.txt") {
      write(lineCount.mkString("\n"))
      close
    }

    val params = EasyReduceParams(
      command = "expr $(cat /input1) + $(cat /input2) > /output",
      imageName = "ubuntu:14.04",
      local = true,
      inputPath = tempDir.getAbsolutePath + "/count_by_line.txt",
      outputPath = tempDir.getAbsolutePath + "/sum.txt",
      fifoReadTimeout = 30)
    EasyReduce.run(params)

    val sc = new SparkContext(conf)
    val sumOut = sc.textFile(tempDir.getAbsolutePath + "/sum.txt").first
    sc.stop

    val sumTest = Source.fromFile(tempDir.getAbsolutePath + "/count_by_line.txt")
      .getLines.map(_.toInt).sum

    assert(sumOut.toInt == sumTest)

  }

  test("easy map, multiple inputs") {

    val params = EasyMapParams(
      command = "rev /input > /output",
      imageName = "ubuntu:14.04",
      local = true,
      inputPath = getClass.getResource("dna").getPath,
      outputPath = tempDir.getAbsolutePath + "/seq.rev",
      wholeFiles = true,
      fifoReadTimeout = 30)
    EasyMap.run(params)

    Seq("dna.txt", "dna1.txt", "dna2.txt").foreach { input =>
      val reverseTest = Source.fromFile(
        getClass.getResource("dna/" + input).getPath)
        .getLines.map(_.reverse)
      val outPath = tempDir.getAbsolutePath + s"/seq.rev/$input"
      val reverseOut = Source.fromFile(
        FilenameUtils.removeExtension(outPath) + ".rev")
        .getLines
      assert(reverseTest.toSet == reverseOut.toSet)
    }

  }

  test("easy reduce, multiple inputs") {

    //Make a test input from DNA
    val sumDir = new File(tempDir.getAbsolutePath + "/sum")
    sumDir.mkdir
    val counts = Seq("dna.txt", "dna1.txt", "dna2.txt").map { input =>
      Source.fromFile(
        getClass.getResource("dna/" + input).getPath)
        .getLines
        .map(_.filter(n => n == 'g' || n == 'c').length)
        .reduce(_+_)
    }
    counts.foreach { count =>
      new PrintWriter(sumDir.getAbsolutePath + s"/${count}.txt") {
        write(count.toString)
        close
      }
    }
    
    val params = EasyReduceParams(
      command = "expr $(cat /input1) + $(cat /input2) > /output",
      imageName = "ubuntu:14.04",
      local = true,
      inputPath = sumDir.getAbsolutePath,
      outputPath = tempDir.getAbsolutePath + "/sum_whole.txt",
      wholeFiles = true,
      fifoReadTimeout = 30)
    EasyReduce.run(params)
    
    val sc = new SparkContext(conf)
    val sumOut = sc.textFile(tempDir.getAbsolutePath + "/sum_whole.txt").first
    sc.stop

    assert(sumOut.toInt == counts.reduce(_+_))

  }

  override def afterAll {
    FileUtils.deleteDirectory(tempDir)
  }

}