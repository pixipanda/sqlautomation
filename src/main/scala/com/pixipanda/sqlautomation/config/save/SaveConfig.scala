package com.pixipanda.sqlautomation.config.save

import com.pixipanda.sqlautomation.config.Options
import com.pixipanda.sqlautomation.config.jdbc.JdbcConfig
import com.typesafe.config.Config

import scala.collection.JavaConverters._


case class SaveConfig(
  sourceType: String,
  repartition: Option[Seq[String]],
  options: Map[String, String]
)

object SaveConfig {

  private def parserRepartition(config: Config): Option[Seq[String]] = {
    if(config.hasPath("repartition")) Some(config.getStringList("repartition").asScala.toList) else None
  }

  def parseSaveConfig(config: Config): SaveConfig = {

    val saveConfig = config.getConfig("save")
    val sourceType =  saveConfig.getString("sourceType")
    val partition = parserRepartition(saveConfig)
    val optionsConfig = saveConfig.getConfig("options")
    val options = Options.parse(optionsConfig)
    val jdbcOptions = JdbcConfig.getJdbcAsMap(sourceType)
    SaveConfig(sourceType, partition, options ++ jdbcOptions)

  }
}
