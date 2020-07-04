package com.pixipanda.handler.transform

case class HiveTransformHandler(query: String, view: String) extends TransformHandler(query, view) {

  override def process(): Unit = {
    val df = spark.sql(query)
    df.createOrReplaceTempView(view)
  }
}
