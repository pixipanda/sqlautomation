package com.pixipanda.sqlautomation.config.transform


import com.typesafe.config.Config

import scala.collection.JavaConverters._

case class SQLConfig(order: Int, fileName: String, sqlTransformConfigs: Seq[SQLTransformConfig])

object SQLConfig {

  def parse(config: Config):SQLConfig = {
    val order = config.getInt("order")
    val filename = config.getString("fileName")
    val sqlTransformConfigs = config.getConfigList("queries").asScala.map(SQLTransformConfig.parse).toList.sortBy(_.order)
    SQLConfig(order, filename, sqlTransformConfigs)
  }

}
