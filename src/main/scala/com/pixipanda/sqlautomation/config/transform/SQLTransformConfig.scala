package com.pixipanda.sqlautomation.config.transform

import com.pixipanda.sqlautomation.config.{ConfigRegistry, ETLConfig, SourceConfig}
import com.pixipanda.sqlautomation.constants.ETL.TRANSFORM
import com.typesafe.config.Config



case class SQLTransformConfig(order: Int,
                              source: Option[SourceConfig],
                              query: String,
                              viewName: Option[String],
                              ETLType: String) extends ETLConfig

object SQLTransformConfig {

  private  def parseViewName(config:Config) = {
    if(config.hasPath("viewName")) Some(config.getString("viewName")) else None
  }

  private def parseSource(config: Config) = {
    if(config.hasPath("source")) {
      Some(SourceConfig.parse(config.getConfig("source")))
    } else None
  }

  def parse(config:Config): SQLTransformConfig = {
    val order = config.getInt("order")
    val source = parseSource(config)
    val queryName = config.getString("queryName")
    val query = ConfigRegistry.getConfig.getString(queryName)
    val viewName = parseViewName(config)
    SQLTransformConfig(order, source, query, viewName, TRANSFORM)
  }
}