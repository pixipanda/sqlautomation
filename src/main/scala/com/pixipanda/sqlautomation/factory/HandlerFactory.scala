package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.extract.ExtractConfig
import com.pixipanda.sqlautomation.config.load.LoadConfig
import com.pixipanda.sqlautomation.config.transform.SQLTransformConfig
import com.pixipanda.sqlautomation.handler.extract.ExtractHandler
import com.pixipanda.sqlautomation.handler.load.LoadHandler
import com.pixipanda.sqlautomation.handler.transform.SQLTransformHandler

object HandlerFactory {

  def getHandler(extractConfig: ExtractConfig): ExtractHandler = {
    val readers = extractConfig.sourceConfigs.map(ReaderFactory.getReader)
    ExtractHandler(readers)
  }

  def getHandler(sqlTransformConfig: SQLTransformConfig): SQLTransformHandler = {
    SQLTransformHandler(sqlTransformConfig.query, sqlTransformConfig.viewName)
  }

  def getHandler(loadConfig: LoadConfig): LoadHandler = {
    val writers = loadConfig.sinkConfigs.map(WriterFactory.getWriter)
    LoadHandler(writers)
  }
}
