package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.config.AppConfig
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.factory.HandlerFactory
import com.pixipanda.sqlautomation.handler.Handler
import com.pixipanda.sqlautomation.handler.extract.EmptyExtractHandler
import org.slf4j.{Logger, LoggerFactory}

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

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def buildPipeline(appConfig: AppConfig): Pipeline[Unit, _ >: Unit with DContainer] = {

    LOGGER.info("Building Pipeline")

    val extractPipeline = appConfig.extractConfig match {
      case Some(extractConfig) =>
        val extractHandler = HandlerFactory.getHandler(extractConfig)
        Pipeline(extractHandler)
      case None => Pipeline(EmptyExtractHandler())
    }

    LOGGER.info(s"Extract Pipeline: $extractPipeline")

    val transformPipeline = appConfig.transformConfig match {
      case Some(transformConfig) =>
        val transformHandlers = HandlerFactory.getHandler(transformConfig)
        transformHandlers.foldLeft(extractPipeline)((pipelineAcc, handler) => {
          pipelineAcc.addHandler(handler)
        })
      case None => extractPipeline
    }

    LOGGER.info(s"Transform Pipeline: $transformPipeline")

    val loadPipeline = appConfig.loadConfig match {
      case Some(loadConfig) =>
        val loadHandler = HandlerFactory.getHandler(loadConfig)
        transformPipeline.addHandler(loadHandler)
      case None => transformPipeline
    }

    LOGGER.info(s"Complete Pipeline: $loadPipeline")

    loadPipeline

  }
}