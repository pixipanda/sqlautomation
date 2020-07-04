package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.queryconfig.QueryConfig
import com.typesafe.config.Config

import scala.collection.JavaConverters._

case class SQLConfig(order: Int, fileName: String, queryConfigs: Seq[QueryConfig])

object SQLConfig{

  def parseSQLConfig(config: Config): Seq[SQLConfig] = {
  val sqlConfigs = config.getConfigList("SQLConfigs").asScala.map(sqlConfig => {
  val order = sqlConfig.getInt("order")
  val fileName = sqlConfig.getString("fileName")
  val queries = QueryConfig.parseQueryConfigs(sqlConfig)
  SQLConfig(order, fileName, queries)
})
  sqlConfigs.sortBy(_.order).toList
}
}
