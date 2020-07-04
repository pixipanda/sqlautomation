package com.pixipanda.sqlautomation.config.queryconfig

import com.pixipanda.constants.ETL.TRANSFORM
import com.typesafe.config.Config

case class TransformConfig (override val order: Int, queryName: String, viewName: String, override val etlType: String) extends QueryConfig(order, etlType)

object TransformConfig {

  def parseTransformConfig(config: Config): Option[TransformConfig] = {
    if(config.hasPath("viewName")) {
      val order = config.getInt("order")
      val query = config.getString("queryName")
      val viewName = config.getString("viewName")
      Some(TransformConfig(order, query, viewName, TRANSFORM))
    } else None
  }
}
