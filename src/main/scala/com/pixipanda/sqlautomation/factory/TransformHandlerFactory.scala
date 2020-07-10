package com.pixipanda.sqlautomation.factory


import com.pixipanda.sqlautomation.handler.transform._
import com.pixipanda.sqlautomation.config.queryconfig._

case class TransformHandlerFactory() extends HandlerFactory {

  def getHandler(queryConfig: QueryConfig): TransformHandler = {
    val transformConfig = queryConfig.asInstanceOf[TransformConfig]
    SparkTransformHandler(transformConfig.query, transformConfig.viewName)
  }
}
