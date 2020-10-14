package com.pixipanda.sqlautomation.config.common

import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

case class SourceConfig(sourceType: String,
                        viewName: Option[String],
                        options: Map[String, String],
                        viewOptions: Option[Map[String, String]],
                        schemaPath: Option[String]
                        )

object SourceConfig {


  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def parseViewName(config: Config): Option[String] = {
    if(config.hasPath("viewName")) Some(config.getString("viewName")) else None
  }

  def parseSchemaPath(config: Config): Option[String] = {
    if(config.hasPath("schemaPath")) Some(config.getString("schemaPath")) else None
  }

  /*
   * This function parses the options of a view. options could be cache, repartition, etc
   */
  def parseViewOptions(config: Config): Option[Map[String, String]] = {

    LOGGER.info("Parsing VeiwOptions")

    if(config.hasPath("viewOptions")) {
      val optionsConfig = config.getConfig("viewOptions")
      val options = Options.parse(optionsConfig)
      Some(options)
    } else
      None
  }

  def parse(config: Config): SourceConfig = {

    LOGGER.info("Parsing SourceConfig")

    val sourceType =  config.getString("sourceType")
    val viewName = parseViewName(config)
    val optionsConfig = config.getConfig("options")
    val options = Options.parse(optionsConfig)
    val viewOptions = parseViewOptions(config)
    val schemaPath = parseSchemaPath(config)

    val sourceConfig = SourceConfig(sourceType, viewName, options, viewOptions, schemaPath)
    LOGGER.info(s"sourceConfig: $sourceConfig")
    sourceConfig
  }
}
