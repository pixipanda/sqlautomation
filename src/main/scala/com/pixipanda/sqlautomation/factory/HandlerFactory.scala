package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.queryconfig.QueryConfig
import com.pixipanda.sqlautomation.handler.Handler
import com.pixipanda.sqlautomation.constants.ETL


trait HandlerFactory {

  def getHandler(queryConfig: QueryConfig): Handler
}

object HandlerFactory{

  def apply(etlType: String): HandlerFactory = {

    etlType match {
      case ETL.LOAD => LoadHandlerFactory()
      case ETL.TRANSFORM  => TransformHandlerFactory()
    }
  }

}