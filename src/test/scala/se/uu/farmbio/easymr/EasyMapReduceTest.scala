package se.uu.farmbio.easymr

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.google.common.io.Files

@RunWith(classOf[JUnitRunner])
class EasyMapReduceTest extends FunSuite {
  
  val conf = new SparkConf()
    .setAppName("MapReduceTest")
    .setMaster("local[*]")
  
  test("easy map") {
    
    val tempDir = Files.createTempDir
    tempDir.deleteOnExit
    
    val params = EasyMapParams(
      command = "rev <input> > <output>",
      imageName = "ubuntu:14.04",
      local = true,
      inputPath = getClass.getResource("dna.txt").getPath,
      outputPath = tempDir.getAbsolutePath + "/rev.txt",
      fifoReadTimeout = 30
    )
    EasyMap.run(params)
    
    val reverseTest = Source.fromFile(getClass.getResource("dna.txt").getPath)
      .getLines.map(_.reverse)
      
    val sc = new SparkContext(conf)
    val reverseOut = sc.textFile(tempDir.getAbsolutePath + "/rev.txt").collect
    sc.stop
    
    assert(reverseTest.toSet == reverseOut.toSet)
    
  }
  
}