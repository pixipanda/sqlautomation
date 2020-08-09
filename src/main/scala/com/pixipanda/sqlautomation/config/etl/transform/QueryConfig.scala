package com.pixipanda.sqlautomation.config.etl.transform

import com.pixipanda.sqlautomation.config.etl.ETLConfig
import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.config.common.SourceConfig
import com.pixipanda.sqlautomation.constants.ETL.TRANSFORM
import com.typesafe.config.Config

case class QueryConfig(order: Int,
                       source: Option[SourceConfig],
                       query: String,
                       viewName: Option[String],
                       ETLType: String) extends ETLConfig

object QueryConfig {

  private  def parseViewName(config:Config) = {
    if(config.hasPath("viewName")) Some(config.getString("viewName")) else None
  }

  private def parseSource(config: Config) = {
    if(config.hasPath("source")) {
      Some(SourceConfig.parse(config.getConfig("source")))
    } else None
  }

  def parse(config:Config): QueryConfig = {
    val order = config.getInt("order")
    val source = parseSource(config)
    val queryName = config.getString("queryName")
    val query = ConfigRegistry.getConfig.getString(queryName)
    val viewName = parseViewName(config)
    QueryConfig(order, source, query, viewName, TRANSFORM)
  }
}
