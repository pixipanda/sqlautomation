package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.queryconfig.{QueryConfig, TransformConfig}
import com.pixipanda.sqlautomation.handler.transform.{HiveTransformHandler, TransformHandler}
import com.typesafe.config.Config

case class TransformHandlerFactory() extends HandlerFactory {

  def getHandler(config: Config, queryConfig: QueryConfig): TransformHandler = {
    val transformConfig = queryConfig.asInstanceOf[TransformConfig]
    val query = config.getString(transformConfig.queryName)
    val viewName = config.getString(transformConfig.viewName)
    HiveTransformHandler(query, viewName)
  }
}
