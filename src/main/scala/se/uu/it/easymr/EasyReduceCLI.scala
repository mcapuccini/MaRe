package se.uu.it.easymr

import org.apache.log4j.Logger

import scopt.OptionParser

case class EasyReduceParams(
  command: String = null,
  imageName: String = null,
  inputPath: String = null,
  outputPath: String = null,
  wholeFiles: Boolean = false,
  local: Boolean = false)

object EasyReduceCLI {

  @transient lazy val log = Logger.getLogger(getClass.getName)

  def run(params: EasyReduceParams) = {

    //Start Spark context
    val sc = EasyContext.create(
      appName = s"Reduce: ${params.command}",
      params.local)

    //Read input data
    val defaultParallelism =
      sc.getConf.get("spark.default.parallelism", "0").toInt
    val data = if (params.wholeFiles) {
      val rdd = if (defaultParallelism > 0) {
        sc.wholeTextFiles(params.inputPath, defaultParallelism)
      } else {
        sc.wholeTextFiles(params.inputPath)
      }
      rdd.map(_._2) //remove file name
    } else {
      if (defaultParallelism > 0) {
        sc.textFile(params.inputPath, defaultParallelism)
      } else {
        sc.textFile(params.inputPath)
      }
    }

    //Reduce data
    val result = new EasyMapReduce(data).reduce(
      params.imageName,
      params.command)

    //Save restult
    sc.parallelize(result.lines.toSeq, 1)
      .saveAsTextFile(params.outputPath)

    //Stop Spark context
    sc.stop

  }

  def main(args: Array[String]) {

    val defaultParams = EasyReduceParams()

    val parser = new OptionParser[EasyReduceParams]("EasyReduce") {
      head("EasyReduce: reduce a distributed dataset using a command from a Docker container.")
      opt[String]("imageName")
        .text("Docker image name.")
        .action((x, c) => c.copy(imageName = x))
      opt[String]("command")
        .required
        .text("command to run inside the Docker container, " +
          "e.g. 'expr $(cat /input1) + $(cat /input2) | tr -d \"\\n\" > /output'. " +
          "The command needs to be associative and commutative.")
        .action((x, c) => c.copy(command = x))
      opt[Unit]("wholeFiles")
        .text("if set, multiple input files will be loaded from an input directory. The command will " +
          "executed in parallel, on the whole files. In contrast, when this is not set " +
          "the file/files in input is/are splitted line by line, and the command is executed in parallel " +
          "on each line of the file.")
        .action((_, c) => c.copy(wholeFiles = true))
      opt[Unit]("local")
        .text("set to run in local mode (useful for testing purpose).")
        .action((_, c) => c.copy(local = true))
      arg[String]("inputPath")
        .required
        .text("dataset input path. Must be a directory if wholeFiles is set.")
        .action((x, c) => c.copy(inputPath = x))
      arg[String]("outputPath")
        .required
        .text("result output path.")
        .action((x, c) => c.copy(outputPath = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }

  }

}