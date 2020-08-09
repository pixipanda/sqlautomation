package com.pixipanda.sqlautomation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

trait Spark {
  implicit lazy val spark: SparkSession = Spark.sparkSingleton
  implicit lazy val sc: SparkContext = spark.sparkContext
  implicit lazy val sqlContext: SQLContext = spark.sqlContext
}

object Spark {

  lazy val sparkSingleton: SparkSession = {

    val sparkConf: SparkConf = new SparkConf()
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.sql.orc.enabled", "true")

    if (!sparkConf.contains("spark.master")) sparkConf.setMaster("local[*]")

    SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

  }
}

