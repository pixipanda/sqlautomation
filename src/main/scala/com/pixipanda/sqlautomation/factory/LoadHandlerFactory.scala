package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.handler.load.jdbc._
import com.pixipanda.sqlautomation.handler.load._
import com.pixipanda.sqlautomation.constants.Source._
import com.pixipanda.sqlautomation.config.queryconfig.{LoadConfig, QueryConfig}

case class LoadHandlerFactory() extends HandlerFactory {

  def getHandler(queryConfig: QueryConfig): LoadHandler = {
    val loadConfig = queryConfig.asInstanceOf[LoadConfig]
    val save = loadConfig.save
    val query = loadConfig.query
    save.sourceType match {
      case HIVE => HiveLoadHandler(query, save)
      case DB2  => DB2LoadHandler(query, save)
      case TERADATA => TeradataLoadHandler(query, save)
    }
  }
}