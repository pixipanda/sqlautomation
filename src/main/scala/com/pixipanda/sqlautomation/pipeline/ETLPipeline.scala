package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.config.SQLAutomate
import com.pixipanda.sqlautomation.factory.HandlerFactory
import com.pixipanda.sqlautomation.handler.Handler


import scala.collection.mutable.ListBuffer

case class ETLPipeline() {

  private val handlers = ListBuffer[Handler]()

  private def addHandlers(handler: Handler): Unit = {

    handlers.append(handler)
  }

  def getHandlers: Seq[Handler] = handlers.toList

  def process(): Unit = {
    handlers.foreach(_.process())
  }
}

object ETLPipeline {

  def buildPipeline(sqlAutomate: SQLAutomate): ETLPipeline = {

    val etlPipeline = ETLPipeline()

    sqlAutomate.sqlConfigs.foreach(sqlConfig => {
      sqlConfig.queryConfigs.foreach(queryConfig => {
        val handlerFactory = HandlerFactory(queryConfig.etlType)
        val handler = handlerFactory.getHandler(queryConfig)
        etlPipeline.addHandlers(handler)
      })
    })
    etlPipeline
  }
}


