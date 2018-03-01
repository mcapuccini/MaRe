package se.uu.it.mare

import java.io.File

import scala.io.Source

import org.apache.spark.SharedSparkContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

private object SDFUtils {
  def parseIDsAndScores(sdf: String) = {
    sdf.split("\\n\\$\\$\\$\\$\\n").map { mol =>
      val lines = mol.split("\\n")
      (lines(0), lines.last)
    }
  }
}

@RunWith(classOf[JUnitRunner])
class VirtualScreeningTest extends FunSuite with SharedSparkContext {

  test("Virtual screening with OEDocking") {

    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "\n$$$$\n")
    val mols = sc.textFile(getClass.getResource("sdf/molecules.sdf").getPath)

    // Parallel execution with MaRe
    val hitsParallel = new MaRe(mols)
      .setInputMountPoint("/input.sdf")
      .setOutputMountPoint("/output.sdf")
      .mapPartitions(
        imageName = "mcapuccini/oe-docking", // obs: this is a private image
        command = "fred -receptor /var/openeye/hiv1_protease.oeb " +
          "-hitlist_size 0 " +
          "-conftest none " +
          "-dbase /input.sdf " +
          "-docked_molecule_file /output.sdf")
      .reducePartitions(
        imageName = "mcapuccini/sdsorter",
        command = "sdsorter -reversesort='FRED Chemgauss4 score' " +
          "-keep-tag='FRED Chemgauss4 score' " +
          "-nbest=3 " +
          "/input.sdf " +
          "/output.sdf")

    // Serial execution
    val inputFile = new File(getClass.getResource("sdf/molecules.sdf").getPath)
    val dockedFile = FileHelper.createTmpFile
    dockedFile.deleteOnExit
    val outputFile = FileHelper.createTmpFile
    outputFile.deleteOnExit
    val docker = new DockerHelper
    docker.run(
      imageName = "mcapuccini/oe-docking", // obs: this is a private image
      command = "fred -receptor /var/openeye/hiv1_protease.oeb " +
        "-hitlist_size 0 " +
        "-conftest none " +
        "-dbase /input.sdf " +
        "-docked_molecule_file /docked.sdf",
      bindFiles = Seq(inputFile, dockedFile),
      volumeFiles = Seq(new File("/input.sdf"), new File("/docked.sdf")))
    docker.run(
      imageName = "mcapuccini/sdsorter",
      command = "sdsorter -reversesort='FRED Chemgauss4 score' " +
        "-keep-tag='FRED Chemgauss4 score' " +
        "-nbest=3 " +
        "/docked.sdf " +
        "/output.sdf",
      bindFiles = Seq(dockedFile, outputFile),
      volumeFiles = Seq(new File("/docked.sdf"), new File("/output.sdf")))
    val hitsSerial = Source.fromFile(outputFile).mkString

    // Test
    val parallel = SDFUtils.parseIDsAndScores(hitsParallel)
    val serial = SDFUtils.parseIDsAndScores(hitsSerial)
    assert(parallel.deep == serial.deep)

  }

}
