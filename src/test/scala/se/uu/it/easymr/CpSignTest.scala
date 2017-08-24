package se.uu.it.easymr

import java.io.ByteArrayInputStream
import java.util.jar.JarInputStream

import org.apache.spark.SharedSparkContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import sun.misc.BASE64Decoder

@RunWith(classOf[JUnitRunner])
class CpSignTest extends FunSuite with SharedSparkContext {

  test("Train ACP in parallel with CPSign") {
    
    val rdd = sc.parallelize(1 to 3).map(_.toString)

    val res = new EasyMapReduce(rdd)
      .setOutputMountPoint("/out.txt")
      .setReduceInputMountPoint1("/model1.txt")
      .setReduceInputMountPoint2("/model2.txt")
      .map(
        imageName = "mcapuccini/cpsign",
        command = "java -jar cpsign-0.5.7.jar train " +
          "-t data_small_train.sdf " +
          "-mn out " +
          "-mo /tmp.cpsign " +
          "-c 1 " +
          "--labels 0 1 " +
          "-rn class " +
          "--license cpsign0.5-standard.license && " +
          "base64 < /tmp.cpsign | tr -d '\n' > /out.txt")
      .reduce(
        imageName = "mcapuccini/cpsign",
        command =
          "base64 -d < /model1.txt > /model1.cpsign && " +
          "base64 -d < /model2.txt > /model2.cpsign && " +
          "java -jar cpsign-0.5.7.jar aggregate " +
          "-m /model1.cpsign /model2.cpsign " + 
          "-mn out " + 
          "-mo /tmp.cpsign " + 
          "-mt 3 " + 
          "--license cpsign0.5-standard.license && " +
          "base64 < /tmp.cpsign | tr -d '\n' > /out.txt")

    // Test that we get a cpsign Jar archive as result
    val base64 = new BASE64Decoder()
    val jarBytes = base64.decodeBuffer(res)
    val jar = new JarInputStream(new ByteArrayInputStream(jarBytes))
    assert(Option(jar.getManifest).isDefined)

  }

}
