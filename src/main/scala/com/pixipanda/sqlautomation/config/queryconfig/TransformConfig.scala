package com.pixipanda.sqlautomation.config.queryconfig

import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.constants.ETL.TRANSFORM
import com.typesafe.config.Config

case class TransformConfig (override val order: Int, query: String, viewName: String, override val etlType: String) extends QueryConfig(order, etlType)

object TransformConfig {

  def parseTransformConfig(config: Config): Option[TransformConfig] = {
    if(config.hasPath("viewName")) {
      val order = config.getInt("order")
      val queryName = config.getString("queryName")
      val query = ConfigRegistry.getConfig.getString(queryName)
      val viewName = config.getString("viewName")
      Some(TransformConfig(order, query, viewName, TRANSFORM))
    } else None
  }
}
