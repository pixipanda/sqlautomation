package com.pixipanda.sqlautomation.handler.transform

import com.pixipanda.sqlautomation.Spark

case class SparkTransformHandler(query: String, view: String) extends TransformHandler(query, view) with Spark{

  override def process(): Unit = {
    val df = spark.sql(query)
    df.createOrReplaceTempView(view)
  }
}
