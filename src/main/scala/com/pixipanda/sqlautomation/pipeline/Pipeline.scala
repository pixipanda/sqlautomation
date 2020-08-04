package com.pixipanda.sqlautomation.pipeline


import com.pixipanda.sqlautomation.config.AppConfig
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.factory.HandlerFactory
import com.pixipanda.sqlautomation.handler.Handler
import com.pixipanda.sqlautomation.handler.extract.EmptyExtractHandler

case class Pipeline[I, O](currentHandler: Handler[I, O]) {

  def addHandler[K](newHandler: Handler[O, K]): Pipeline[I, K] = {

    Pipeline(new Handler[I,K] {
      override def process(input: I): K = {
        newHandler.process(currentHandler.process(input))
      }
    })
  }

  def process(input: I): O = {
    currentHandler.process(input)
  }
}


object Pipeline {

  def buildPipeline(appConfig: AppConfig):Pipeline[Unit, DContainer] = {

    val pipeline = appConfig.extractConfig match {
      case Some(extractConfig) =>
        val extractHandler = HandlerFactory.getHandler(extractConfig)
        Pipeline(extractHandler)
      case None => Pipeline(new EmptyExtractHandler())
    }

    appConfig.transformConfig match {
      case Some(transformConfig) =>
        transformConfig.sqlConfigs.foreach(sqlConfig => {
          sqlConfig.sqlTransformConfigs.foreach(sqlTransformConfig => {
            val sqlTransformHandler = HandlerFactory.getHandler(sqlTransformConfig)
            pipeline.addHandler(sqlTransformHandler)
          })
        })
    }

    val loadHandler = HandlerFactory.getHandler(appConfig.loadConfig)
    pipeline.addHandler(loadHandler)
    pipeline

  }
}