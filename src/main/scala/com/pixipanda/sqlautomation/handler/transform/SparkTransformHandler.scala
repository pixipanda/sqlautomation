package com.pixipanda.sqlautomation.handler.transform

case class SparkTransformHandler(query: String, view: String) extends TransformHandler(query, view) {

  override def process(): Unit = {
    val df = spark.sql(query)
    df.createOrReplaceTempView(view)
  }
}
