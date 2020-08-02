package com.pixipanda.sqlautomation.handler.load

import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.sqlautomation.Spark
import org.apache.spark.sql.functions.col

case class HiveLoadHandler(query: String, saveConfig: SaveConfig) extends LoadHandler(query, saveConfig) with Spark {

  override def process(): Unit = {

    val df = spark.sql(query)
    val db = saveConfig.options("db")
    val table = saveConfig.options("table")


    saveConfig.repartition match {
      case Some(columns) =>
        df.repartition(columns.map(col): _*)
          .write
          .format(saveConfig.options("format"))
          .mode(saveConfig.options("mode"))
          .partitionBy(columns: _*)
          .insertInto(s"$db.$table")
      case None =>
        df.write
          .format(saveConfig.options("format"))
          .mode(saveConfig.options("mode"))
          .insertInto(s"$db.$table")
    }
  }
}