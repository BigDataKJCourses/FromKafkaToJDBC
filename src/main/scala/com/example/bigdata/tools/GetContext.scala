package com.example.bigdata.tools

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetContext {
  def getSparkSession(appName: String): SparkSession = {
    val master = sys.props.getOrElse("spark.master", "local[*]")

    val conf: SparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark
  }
}
