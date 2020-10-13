package com.pixipanda.sqlautomation.config.common

import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

case class SourceConfig(sourceType: String, viewName: Option[String], options: Map[String, String], schemaPath: Option[String])

object SourceConfig {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)


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

    val sourceConfig = SourceConfig(sourceType, viewName, options, schemaPath)
    LOGGER.info(s"sourceConfig: $sourceConfig")
    sourceConfig
  }
}
