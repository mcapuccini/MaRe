package se.uu.it.easymr

import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class EasyMapReduceTest
    extends FunSuite
    with SharedSparkContext {

  test("Map (DNA reverse)") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath)

    val resRDD = EasyMapReduce.map(
      rdd,
      imageName = "busybox",
      command = "rev /input > /output | tr -d '\\n'")

    rdd.collect.zip(resRDD.collect).foreach {
      case (seq1, seq2) => assert(seq1.reverse == seq2)
    }

  }

  test("Reduce (GC count)") {

    val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath)
      .map(_.count(c => c == "G" || c == "C").toString)

    val res = EasyMapReduce.reduce(
      rdd,
      imageName = "busybox",
      command = "expr $(cat /input1) + $(cat /input2) | tr -d '\\n' > /output")

    val sum = rdd.reduce {
      case (lineCount1, lineCount2) =>
        (lineCount1.toInt + lineCount2.toInt).toString
    }
    assert(sum == res)

  }

}