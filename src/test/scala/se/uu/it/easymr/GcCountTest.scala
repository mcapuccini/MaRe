package se.uu.it.easymr

import scala.io.Source

import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GcCountTest extends FunSuite with SharedSparkContext {

  test("GC count in DNA string") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath)

    val res = new EasyMapReduce(rdd)
      .map(
        imageName = "ubuntu:xenial",
        command = "grep -o '[gc]' /input | wc -l > /output")
      .reduce(
        imageName = "ubuntu:xenial",
        command = "awk '{s+=$1} END {print s}' /input > /output")

    // Check if results matches with the one computed with the standard RDD API        
    val toMatch = sc.textFile(getClass.getResource("dna/dna.txt").getPath)
      .map(_.count(c => c == 'g' || c == 'c').toString)
      .reduce {
        case (lineCount1, lineCount2) =>
          (lineCount1.toInt + lineCount2.toInt).toString
      }
    assert(res == toMatch)

  }

}