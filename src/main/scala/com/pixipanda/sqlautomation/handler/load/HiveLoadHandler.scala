package com.pixipanda.sqlautomation.handler.load

import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.sqlautomation.Spark

case class HiveLoadHandler(query: String, saveConfig: SaveConfig) extends LoadHandler(query, saveConfig) with Spark {

  override def process(): Unit = {

    val df = spark.sql(query)
    val db = saveConfig.options("db")
    val table = saveConfig.options("table")


    saveConfig.partition match {
      case Some(columns) =>
        df.write
          .format(saveConfig.options("format"))
          .mode(saveConfig.options("mode"))
          .partitionBy(columns: _*)
          .saveAsTable(s"$db.$table")
      case None =>
        df.write
          .format(saveConfig.options("format"))
          .mode(saveConfig.options("mode"))
          .saveAsTable(s"$db.$table")
    }
  }
}