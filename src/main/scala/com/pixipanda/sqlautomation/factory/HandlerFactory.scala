package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.etl.extract.ExtractConfig
import com.pixipanda.sqlautomation.config.etl.load.LoadConfig
import com.pixipanda.sqlautomation.config.etl.transform.TransformConfig
import com.pixipanda.sqlautomation.handler.extract.ExtractHandler
import com.pixipanda.sqlautomation.handler.load.LoadHandler
import com.pixipanda.sqlautomation.handler.transform.QueryTransformHandler

object HandlerFactory {

  /*
   Given extract config. it will return extact handler
  */
  def getHandler(extractConfig: ExtractConfig): ExtractHandler = {
    val readers = extractConfig.sourceConfigs.map(ReaderFactory.getReader)
    ExtractHandler(readers)
  }

  /*
   Given transform config. it will return a collection of transform handler
  */
  def getHandler(transformConfig: TransformConfig): Seq[QueryTransformHandler] = {
    transformConfig.sqlConfigs.flatMap(sqlConfig => {
      sqlConfig.queryConfigs.map(queryConfig => {
        val query = queryConfig.query
        val vieName = queryConfig.viewName
        QueryTransformHandler(query, vieName)
      })
    })
  }

  /*
   Given load config. it will return load handler
  */
  def getHandler(loadConfig: LoadConfig): LoadHandler = {
    val writers = loadConfig.sinkConfigs.map(WriterFactory.getWriter)
    LoadHandler(writers)
  }
}