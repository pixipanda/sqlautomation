package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.queryconfig.QueryConfig
import com.pixipanda.sqlautomation.handler.Handler
import com.pixipanda.sqlautomation.constants.ETL
import com.typesafe.config.Config

trait HandlerFactory {

  def getHandler(config: Config, queryConfig: QueryConfig): Handler
}

object HandlerFactory{

  def apply(queryConfig: QueryConfig): HandlerFactory = {

    queryConfig.etlType match {
      case ETL.LOAD => new LoadHandlerFactory
      case ETL.TRANSFORM  => new TransformHandlerFactory
    }
  }

}