package se.uu.it.easymr

import java.io.ByteArrayInputStream
import java.util.jar.JarInputStream

import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.compact
import org.json4s.native.JsonMethods.parse
import org.json4s.native.JsonMethods.render
import org.json4s.string2JsonInput
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import sun.misc.BASE64Decoder

@RunWith(classOf[JUnitRunner])
class CpSignTest extends FunSuite with SharedSparkContext {

  //  test("Train ACP in parallel with CPSign") {
  //
  //    val rdd = sc.parallelize(1 to 3).map(_.toString)
  //
  //    val res = new EasyMapReduce(rdd)
  //      .setOutputMountPoint("/out.txt")
  //      .setReduceInputMountPoint1("/model1.txt")
  //      .setReduceInputMountPoint2("/model2.txt")
  //      .map(
  //        imageName = "mcapuccini/cpsign",
  //        command = "java -jar cpsign-0.6.1.jar train " +
  //          "-t data_small_train.sdf " +
  //          "-mn out " +
  //          "-mo /tmp.cpsign " +
  //          "-c 1 " +
  //          "--labels 0 1 " +
  //          "-rn class " +
  //          "--license cpsign0.6-standard.license && " +
  //          "[ -e tmp.cpsign ] && " + // workaround for cpsign bug (it always exits with 0)
  //          "base64 < /tmp.cpsign | tr -d '\n' > /out.txt")
  //      .reduce(
  //        imageName = "mcapuccini/cpsign",
  //        command =
  //          "base64 -d < /model1.txt > /model1.cpsign && " +
  //            "base64 -d < /model2.txt > /model2.cpsign && " +
  //            "java -jar cpsign-0.6.1.jar fast-aggregate " +
  //            "-m /model1.cpsign /model2.cpsign " +
  //            "-mo /tmp.cpsign " +
  //            "--license cpsign0.6-standard.license && " +
  //            "[ -e tmp.cpsign ] && " + // workaround for cpsign bug (it always exits with 0)
  //            "base64 < /tmp.cpsign | tr -d '\n' > /out.txt")
  //
  //    // Test that we get a cpsign Jar archive as result
  //    val base64 = new BASE64Decoder()
  //    val jarBytes = base64.decodeBuffer(res)
  //    val jar = new JarInputStream(new ByteArrayInputStream(jarBytes))
  //    assert(Option(jar.getManifest).isDefined)
  //
  //  }

  test("Train ACP in parallel with CPSign, aggregate predictions only") {

    val seeds = Seq(2200, 7872, 1935)
    val rdd = sc.parallelize(seeds.map(_.toString))

    // Define median primitive
    val median = (seq: Seq[Double]) => if (seq.length % 2 == 0) {
      val sort = seq.sortWith(_ < _)
      (sort(sort.length / 2) + sort(sort.length / 2 - 1)) / 2
    } else {
      seq.sortWith(_ < _)(seq.length / 2)
    }

    val predictions = new EasyMapReduce(rdd)
      .setInputMountPoint("/in.txt")
      .setOutputMountPoint("/out.txt")
      // Train
      .map(
        imageName = "mcapuccini/cpsign",
        command = "java -jar cpsign-0.6.1.jar train " +
          "-t data_small_train.sdf " +
          "-mn out " +
          "-mo /tmp.cpsign " +
          "-c 1 " +
          "--labels 0 1 " +
          "-rn class " +
          "--seed $(cat /in.txt | tr -d '\n') " +
          "--license cpsign0.6-standard.license && " +
          "[ -e tmp.cpsign ] && " + // workaround for cpsign bug (it always exits with 0)
          "base64 < /tmp.cpsign | tr -d '\n' > /out.txt")
      // Predict
      .map(
        imageName = "mcapuccini/cpsign",
        command = "base64 -d < /in.txt > /model.cpsign && " +
          "java -jar cpsign-0.6.1.jar predict " +
          "-m /model.cpsign " +
          "-p data_small_test.sdf " +
          "-c 1 " +
          "-co 0.8 " +
          "-o /out.txt " +
          "--license cpsign0.6-standard.license")
      .getRDD.map { json =>
        val parsedJson = parse(json)
        val key = compact(render(parsedJson \ "molecule" \ "SMILES"))
        val pv0 = compact(render(parsedJson \ "prediction" \ "pValues" \ "0")).toDouble
        val pv1 = compact(render(parsedJson \ "prediction" \ "pValues" \ "1")).toDouble
        (key, (Seq(pv0), Seq(pv1)))
      }

    // Check for reproducibility
    val predGroup1 = predictions
      .reduceByKey { case ((seq0a, seq1a), (seq0b, seq1b)) => (seq0a ++ seq0b, seq1a ++ seq1b) }
      .map { case (title, (s0, s1)) => (title, median(s0), median(s1)) }
      .collect
    val predGroup2 = predictions
      .reduceByKey { case ((seq0a, seq1a), (seq0b, seq1b)) => (seq0a ++ seq0b, seq1a ++ seq1b) }
      .map { case (title, (s0, s1)) => (title, median(s0), median(s1)) }
      .collect
    assert(predGroup1.deep == predGroup2.deep)

    // Check that we get the correct number of predictions
    assert(predGroup1.length == 161L)

  }

}
