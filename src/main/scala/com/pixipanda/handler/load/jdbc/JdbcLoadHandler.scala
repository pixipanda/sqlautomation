package com.pixipanda.handler.load.jdbc

import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.handler.load.LoadHandler
import org.apache.spark.sql.functions.col

abstract class JdbcLoadHandler(query: String, saveConfig:SaveConfig) extends LoadHandler(query, saveConfig) {

  override def process(): Unit = {
    val df = spark.sql(query)
    saveConfig.partition.foreach(columns => {
      df.repartition(columns.map(col): _*)
        .write
        .options(saveConfig.options)
        .save(saveConfig.path)
    })
  }


  def decryptPassword:String = {
    ""
  }
}
