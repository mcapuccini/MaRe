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
  
  test("map file") {
    
    val tempDir = Files.createTempDir
    tempDir.deleteOnExit
    
    val params = EasyMapParams(
      command = "rev <input> > <output>",
      local = true,
      inputPath = getClass.getResource("test.txt").getPath,
      outputPath = tempDir.getAbsolutePath + "/test.out",
      fifoReadTimeout = 30
    )
    EasyMap.run(params)
    
    val reverseTest = Source.fromFile(getClass.getResource("test.txt").getPath)
      .getLines.map(_.reverse)
      
    val sc = new SparkContext(conf)
    val reverseOut = sc.textFile(tempDir.getAbsolutePath + "/test.out").collect
    sc.stop
    
    assert(reverseTest.toSet == reverseOut.toSet)
    
  }
  
}