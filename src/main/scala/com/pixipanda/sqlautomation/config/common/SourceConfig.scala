package com.pixipanda.sqlautomation.config.common

import com.typesafe.config.Config
import org.apache.log4j.Logger

case class SourceConfig(sourceType: String, viewName: Option[String], options: Map[String, String], schemaPath: Option[String])

object SourceConfig {

  val LOGGER: Logger = Logger.getLogger(getClass.getName)

  def parseViewName(config: Config): Option[String] = {
    if(config.hasPath("viewName")) Some(config.getString("viewName")) else None
  }

  def parseSchemaPath(config: Config): Option[String] = {
    if(config.hasPath("schemaPath")) Some(config.getString("schemaPath")) else None
  }

  def parse(config: Config): SourceConfig = {
    val sourceType =  config.getString("sourceType")
    val viewName = parseViewName(config)
    val optionsConfig = config.getConfig("options")
    val options = Options.parse(optionsConfig)
    val schemaPath = parseSchemaPath(config)

    LOGGER.info(s"options: $options")

    SourceConfig(sourceType, viewName, options, schemaPath)
  }
}
