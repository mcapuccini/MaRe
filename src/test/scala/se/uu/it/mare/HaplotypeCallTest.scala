package se.uu.it.mare

import java.io.File
import java.util.UUID

import scala.io.Source
import scala.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.Text
import org.apache.spark.SharedSparkContext
import org.bdgenomics.adam.io.SingleFastqInputFormat
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HaplotypeCallTest extends FunSuite with SharedSparkContext {

  private val tmpDir = new File(Properties.envOrElse("TMPDIR", "/tmp"))

  test("SNP callign") {

    // Parallel execution with MaRe
    // Read and interlace dataset
    val fr = sc.newAPIHadoopFile(
      getClass.getResource("dna/fastq/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq").getPath,
      classOf[SingleFastqInputFormat],
      classOf[Void],
      classOf[Text]).map(_._2.toString)
    val rr = sc.newAPIHadoopFile(
      getClass.getResource("dna/fastq/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq").getPath,
      classOf[SingleFastqInputFormat],
      classOf[Void],
      classOf[Text]).map(_._2.toString)
    val inputData = fr.zip(rr).map { case (fr, rr) => fr + rr.dropRight(1) }.repartition(4)
    // Run MaRe
    val resPar = new MaRe(inputData).map(
      inputMountPoint = TextFile("/chunk.fastq"),
      outputMountPoint = TextFile("/aln.sam"),
      imageName = "mcapuccini/alignment:latest-1-16",
      command = """
        set -e
        reference_genome=/ref/human_g1k_v37.1-16.fasta
        reads=/chunk.fastq
        bwa mem -t 1 -p $reference_genome $reads | samtools view > /aln.sam
        """)
      .repartitionBy(
        keyBy = (aln: String) => {
          val chrStr = aln.split("\\s+")(2)
          if (chrStr.forall(Character.isDigit)) {
            chrStr.toInt % 4
          } else {
            chrStr match {
              case "X" => 23 % 4
              case "Y" => 24 % 4
              case "MT" => 25 % 4
              case _ => 26 % 4
            }
          }
        },
        numPartitions = 4)
      .map(
        inputMountPoint = TextFile("/aln.sam"),
        outputMountPoint = BinaryFiles("/results"),
        imageName = "mcapuccini/alignment:latest-1-16",
        command = """
            set -e
            cat /ref/human_g1k_v37.1-16.dict /aln.sam > /aln.header.sam
            UUID=$(cat /proc/sys/kernel/random/uuid)
            gatk AddOrReplaceReadGroups \
              --INPUT=/aln.header.sam \
              --OUTPUT=/aln.header.sorted.rg.bam \
              --SORT_ORDER=coordinate \
              --RGID=H06HDADXX130110-id \
              --RGLB=H06HDADXX130110-lib \
              --RGPL=ILLUMINA \
              --RGPU=H06HDADXX130110-01 \
              --RGSM=H06HDADXX130110
            gatk BuildBamIndex --INPUT=/aln.header.sorted.rg.bam
            reference_genome=/ref/human_g1k_v37.1-16.fasta
            gatk HaplotypeCallerSpark -R $reference_genome \
              -I /aln.header.sorted.rg.bam \
              -O /results/aln.${UUID}.g.vcf
            gzip /results/*
            """)
      .reduce(
        inputMountPoint = BinaryFiles("/in"),
        outputMountPoint = BinaryFiles("/out"),
        imageName = "opengenomics/vcftools-tools:latest",
        command = """
            set -e
            UUID=$(cat /proc/sys/kernel/random/uuid)
            vcf-concat /in/*.vcf.gz | gzip -c > /out/results.${UUID}.g.vcf.gz
            """)
      .map(
        inputMountPoint = BinaryFiles("/in"),
        outputMountPoint = TextFile("/out.tsv"),
        imageName = "mcapuccini/alignment:latest-1-16",
        command = """
            set -e
            gunzip /in/*.gz
            gatk VariantsToTable -V /in/*.vcf -O /out.hdr.tsv -F CHROM -F POS
            tail -n +2 /out.hdr.tsv > /out.tsv
            """).rdd.collect

    // Serial execution
    val inputDir = new File(getClass.getResource("dna/fastq").getPath)
    val resultsDir = new File(tmpDir, "mare_test_" + UUID.randomUUID.toString)
    resultsDir.mkdir
    FileUtils.forceDeleteOnExit(resultsDir)
    DockerHelper.run(
      imageName = "mcapuccini/alignment:latest-1-16",
      command = """
            set -e
            reference_genome=/ref/human_g1k_v37.1-16.fasta
            bwa mem -t 1 $reference_genome \
              /input/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq \
              /input/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq \
              | samtools view > /aln.sam
            cat /ref/human_g1k_v37.1-16.dict /aln.sam > /aln.header.sam
            gatk AddOrReplaceReadGroups \
              --INPUT=/aln.header.sam \
              --OUTPUT=/aln.header.sorted.rg.bam \
              --SORT_ORDER=coordinate \
              --RGID=H06HDADXX130110-id \
              --RGLB=H06HDADXX130110-lib \
              --RGPL=ILLUMINA \
              --RGPU=H06HDADXX130110-01 \
              --RGSM=H06HDADXX130110
            gatk BuildBamIndex --INPUT=/aln.header.sorted.rg.bam
            gatk HaplotypeCallerSpark -R $reference_genome \
              -I /aln.header.sorted.rg.bam \
              -O /results/aln.g.vcf
            """,
      bindFiles = Seq(inputDir, resultsDir),
      volumeFiles = Seq(new File("/input"), new File("/results")),
      forcePull = false)
    DockerHelper.run(
      imageName = "opengenomics/vcftools-tools:latest",
      command = "vcf-concat /results/*.vcf > /results/results.g.vcf",
      bindFiles = Seq(resultsDir),
      volumeFiles = Seq(new File("/results")),
      forcePull = false)
    DockerHelper.run(
      imageName = "mcapuccini/alignment:latest-1-16",
      command = """
            set -e
            gatk VariantsToTable -V /results/results.g.vcf -O /results/out.hdr.tsv -F CHROM -F POS
            tail -n +2 /results/out.hdr.tsv > /results/out.tsv
          """,
      bindFiles = Seq(resultsDir),
      volumeFiles = Seq(new File("/results")),
      forcePull = false)
    val resSer = Source.fromFile(new File(resultsDir, "out.tsv")).getLines

    // Test
    assert(resPar.filter(_(0) != 'G').sorted.deep == resSer.filter(_(0) != 'G').toArray.sorted.deep)

  }

}
