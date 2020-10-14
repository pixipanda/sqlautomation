package com.pixipanda.sqlautomation.config.etl.transform

import com.pixipanda.sqlautomation.config.etl.ETLConfig
import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.config.common.{Options, SourceConfig}
import com.pixipanda.sqlautomation.constants.ETL.TRANSFORM
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

case class QueryConfig(order: Int,
                       source: Option[SourceConfig],
                       query: String,
                       viewName: Option[String],
                       viewOptions: Option[Map[String, String]],
                       ETLType: String) extends ETLConfig

object QueryConfig {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  private  def parseViewName(config:Config) = {
    if(config.hasPath("viewName")) Some(config.getString("viewName")) else None
  }

  private def parseSource(config: Config) = {
    if(config.hasPath("source")) {
      Some(SourceConfig.parse(config.getConfig("source")))
    } else None
  }

  def parseViewOptions(config: Config): Option[Map[String, String]] = {

    LOGGER.info("Parsing VeiwOptions")

    if(config.hasPath("viewOptions")) {
      val optionsConfig = config.getConfig("viewOptions")
      val options = Options.parse(optionsConfig)
      Some(options)
    } else
      None
  }

  def parse(config:Config): QueryConfig = {

    LOGGER.info(s"Parsing QueryConfig")

    val order = config.getInt("order")
    val source = parseSource(config)
    val queryName = config.getString("queryName")
    val query = ConfigRegistry.getConfig.getString(queryName)
    val viewName = parseViewName(config)
    val viewOptions = parseViewOptions(config)
    val queryConfig = QueryConfig(order, source, query, viewName, viewOptions, TRANSFORM)
    LOGGER.info(s"queryConfig: $queryConfig")
    queryConfig
  }
}
