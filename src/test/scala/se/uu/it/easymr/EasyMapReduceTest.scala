package se.uu.it.easymr

import scala.io.Source

import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EasyMapReduceTest
    extends FunSuite
    with SharedSparkContext {

  test("Map (DNA reverse)") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath)

    val resRDD = new EasyMapReduce(rdd).map(
      imageName = "ubuntu:xenial",
      command = "rev /input > /output")
      .getRDD

    rdd.collect.zip(resRDD.collect).foreach {
      case (seq1, seq2) => {
        assert(seq1.reverse == seq2)
      }
    }

  }
  
  test("Map partitions (DNA reverse)") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath)

    val resRDD = new EasyMapReduce(rdd).mapPartitions(
      imageName = "ubuntu:xenial",
      command = "rev /input > /output")
      .getRDD

    rdd.collect.zip(resRDD.collect).foreach {
      case (seq1, seq2) => {
        assert(seq1.reverse == seq2)
      }
    }

  }

  test("Map whole files (DNA reverse)") {

    val rdd = sc.wholeTextFiles(getClass.getResource("dna").getPath)

    val resRDD = EasyMapReduce.mapWholeFiles(
      rdd,
      imageName = "ubuntu:xenial",
      command = "rev /input > /output")

    rdd.collect.zip(resRDD.collect).foreach {
      case ((_, seq1), (_, seq2)) =>
        val toMatch = Source.fromString(seq1)
          .getLines
          .map(_.reverse)
          .mkString("\n")
        assert(toMatch == seq2)
    }

  }

  test("Reduce (GC count)") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath)
      .map(_.count(c => c == 'g' || c == 'c').toString)

    val res = new EasyMapReduce(rdd).reduce(
      imageName = "ubuntu:xenial",
      command = "expr $(cat /input1) + $(cat /input2) | tr -d '\\n' > /output")

    val sum = rdd.reduce {
      case (lineCount1, lineCount2) =>
        (lineCount1.toInt + lineCount2.toInt).toString
    }
    assert(sum == res)

  }

  test("MapReduce (GC count)") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath)

    val res = new EasyMapReduce(rdd)
      .map(
        imageName = "ubuntu:xenial",
        command = "grep -o '[gc]' /input | wc -l > /output")
      .reduce(
        imageName = "ubuntu:xenial",
        command = "expr $(cat /input1) + $(cat /input2) | tr -d '\\n' > /output")

    val toMatch = sc.textFile(getClass.getResource("dna/dna.txt").getPath)
      .map(_.count(c => c == 'g' || c == 'c').toString)
      .reduce {
        case (lineCount1, lineCount2) =>
          (lineCount1.toInt + lineCount2.toInt).toString
      }
    
    assert(res == toMatch)

  }

}