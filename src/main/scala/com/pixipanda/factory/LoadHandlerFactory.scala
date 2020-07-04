package com.pixipanda.factory

import com.pixipanda.constants.Source._
import com.pixipanda.sqlautomation.config.queryconfig.{LoadConfig, QueryConfig}
import com.pixipanda.handler.load.jdbc.{DB2LoadHandler, TeradataLoadHandler}
import com.pixipanda.handler.load.{HiveLoadHandler, LoadHandler}
import com.typesafe.config.Config

class LoadHandlerFactory extends HandlerFactory {

  def getHandler(config: Config, queryConfig: QueryConfig): LoadHandler = {
    val loadConfig = queryConfig.asInstanceOf[LoadConfig]
    val query = config.getString(loadConfig.queryName)
    val save = loadConfig.save
    save.sourceType match {
      case HIVE => HiveLoadHandler(query, save)
      case DB2  => DB2LoadHandler(query, save)
      case TERADATA => TeradataLoadHandler(query, save)
    }
  }
}