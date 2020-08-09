package com.pixipanda.sqlautomation.config.etl.transform

import com.typesafe.config.Config

import scala.collection.JavaConverters._

case class SQLConfig(order: Int, fileName: String, queryConfigs: Seq[QueryConfig])

object SQLConfig {

  def parse(config: Config):SQLConfig = {
    val order = config.getInt("order")
    val filename = config.getString("fileName")
    val sqlTransformConfigs = config.getConfigList("queries").asScala.map(QueryConfig.parse).toList.sortBy(_.order)
    SQLConfig(order, filename, sqlTransformConfigs)
  }
}