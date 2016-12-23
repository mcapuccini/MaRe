package se.uu.it.easymr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

private[easymr] object EasyContext {

  def create(appName: String, local: Boolean) = {
    val conf = new SparkConf()
      .setAppName(appName)
    if (local) {
      conf.setMaster("local[2]")
      conf.set("spark.default.parallelism", "2")
    }
    if (System.getenv("EASYMR_TMP") != null) {
      conf.setExecutorEnv("EASYMR_TMP", System.getenv("EASYMR_TMP"))
    }
    if (System.getenv("TMPDIR") != null) {
      conf.setExecutorEnv("TMPDIR", System.getenv("TMPDIR"))
    }
    new SparkContext(conf)
  }

}