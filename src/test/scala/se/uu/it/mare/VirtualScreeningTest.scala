package se.uu.it.mare

import java.io.File
import java.util.UUID

import scala.io.Source
import scala.util.Properties

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

  private val tmpDir = new File(Properties.envOrElse("TMPDIR", "/tmp"))

  test("Virtual Screening") {

    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "\n$$$$\n")
    val mols = sc.textFile(getClass.getResource("sdf/molecules.sdf").getPath)

    // Parallel execution with MaRe
    val hitsParallel = new MaRe(mols)
      .map(
        inputMountPoint = TextFile("/input.sdf", "\n$$$$\n"),
        outputMountPoint = TextFile("/output.sdf", "\n$$$$\n"),
        imageName = "mcapuccini/oe:latest",
        command = "fred -receptor /var/openeye/hiv1_protease.oeb " +
          "-hitlist_size 0 " +
          "-conftest none " +
          "-dock_resolution Low " +
          "-dbase /input.sdf " +
          "-docked_molecule_file /output.sdf")
      .reduce(
        inputMountPoint = TextFile("/input.sdf", "\n$$$$\n"),
        outputMountPoint = TextFile("/output.sdf", "\n$$$$\n"),
        imageName = "mcapuccini/sdsorter:latest",
        command = "sdsorter -reversesort='FRED Chemgauss4 score' " +
          "-keep-tag='FRED Chemgauss4 score' " +
          "-nbest=30 " +
          "/input.sdf " +
          "/output.sdf")
      .rdd.collect.mkString("\n$$$$\n")

    // Serial execution
    val inputFile = new File(getClass.getResource("sdf/molecules.sdf").getPath)
    val dockedFile = new File(tmpDir, "mare_test_" + UUID.randomUUID.toString)
    dockedFile.createNewFile
    dockedFile.deleteOnExit
    val outputFile = new File(tmpDir, "mare_test_" + UUID.randomUUID.toString)
    outputFile.createNewFile
    outputFile.deleteOnExit
    DockerHelper.run(
      imageName = "mcapuccini/oe:latest",
      command = "fred -receptor /var/openeye/hiv1_protease.oeb " +
        "-hitlist_size 0 " +
        "-conftest none " +
        "-dock_resolution Low " +
        "-dbase /input.sdf " +
        "-docked_molecule_file /docked.sdf",
      bindFiles = Seq(inputFile, dockedFile),
      volumeFiles = Seq(new File("/input.sdf"), new File("/docked.sdf")),
      forcePull = false)
    DockerHelper.run(
      imageName = "mcapuccini/sdsorter:latest",
      command = "sdsorter -reversesort='FRED Chemgauss4 score' " +
        "-keep-tag='FRED Chemgauss4 score' " +
        "-nbest=30 " +
        "/docked.sdf " +
        "/output.sdf",
      bindFiles = Seq(dockedFile, outputFile),
      volumeFiles = Seq(new File("/docked.sdf"), new File("/output.sdf")),
      forcePull = false)
    val hitsSerial = Source.fromFile(outputFile).mkString

    // Test
    val parallel = SDFUtils.parseIDsAndScores(hitsParallel)
    val serial = SDFUtils.parseIDsAndScores(hitsSerial)
    assert(parallel.deep == serial.deep)

  }

}