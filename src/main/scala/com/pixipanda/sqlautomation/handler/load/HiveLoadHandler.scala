package com.pixipanda.sqlautomation.handler.load

import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.sqlautomation.Spark
import org.apache.spark.sql.functions.col

case class HiveLoadHandler(query: String, saveConfig: SaveConfig) extends LoadHandler(query, saveConfig) with Spark {

  override def process(): Unit = {

    val df = spark.sql(query)
    val db = saveConfig.options("db")
    val table = saveConfig.options("table")

    val partitionedDF = saveConfig.partition match {
      case Some(columns) => df.repartition(columns.map(col): _*)
      case None => df
    }

    partitionedDF
      .write
      .format(saveConfig.options("format"))
      .mode(saveConfig.options("mode"))
      .saveAsTable(s"$db.$table")
  }
}