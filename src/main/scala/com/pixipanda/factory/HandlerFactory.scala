package com.pixipanda.factory

import com.pixipanda.sqlautomation.config.queryconfig.QueryConfig
import com.pixipanda.constants.ETL
import com.pixipanda.handler.Handler
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