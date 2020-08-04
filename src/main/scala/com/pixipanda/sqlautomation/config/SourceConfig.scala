package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.jdbc.JdbcConfig
import com.typesafe.config.Config

case class SourceConfig(sourceType: String, viewName: Option[String], options: Map[String, String])

object SourceConfig {

  def parseViewName(config: Config): Option[String] = {
    if(config.hasPath("viewName")) Some(config.getString("viewName")) else None
  }

  def parse(config: Config): SourceConfig = {
    val sourceType =  config.getString("sourceType")
    val viewName = parseViewName(config)
    val optionsConfig = config.getConfig("options")
    val options = Options.parse(optionsConfig)
    val jdbcOptions = JdbcConfig.getJdbcAsMap(sourceType)
    SourceConfig(sourceType, viewName, options ++ jdbcOptions)
  }
}