package com.pixipanda.sqlautomation.config.queryconfig

import com.typesafe.config.Config

import scala.collection.JavaConverters._

abstract class QueryConfig(val order: Int, val etlType: String)


object QueryConfig {


  def getQueryConfig(config: Config): QueryConfig = {

    TransformConfig.parseTransformConfig(config) match {
      case Some(transformConfig) => transformConfig
      case None => LoadConfig.parseLoadConfig(config) match {
        case Some(loadConfig) => loadConfig
      }
    }
  }

  def parseQueryConfigs(config: Config): Seq[QueryConfig] = {
    if(config.hasPath("queries")) {
      val queries = config.getConfigList("queries").asScala.map(getQueryConfig)
      queries.sortBy(_.order).toList
    } else {
      Seq.empty[QueryConfig]
    }
  }

}