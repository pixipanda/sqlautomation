package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.extract.ExtractConfig
import com.pixipanda.sqlautomation.config.load.LoadConfig
import com.pixipanda.sqlautomation.config.transform.TransformConfig
import com.pixipanda.sqlautomation.handler.extract.ExtractHandler
import com.pixipanda.sqlautomation.handler.load.LoadHandler
import com.pixipanda.sqlautomation.handler.transform.SQLTransformHandler

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
  def getHandler(transformConfig: TransformConfig): Seq[SQLTransformHandler] = {
    transformConfig.sqlConfigs.flatMap(sqlConfig => {
      sqlConfig.sqlTransformConfigs.map(sqlTransformConfig => {
        val query = sqlTransformConfig.query
        val vieName = sqlTransformConfig.viewName
        SQLTransformHandler(query, vieName)
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
