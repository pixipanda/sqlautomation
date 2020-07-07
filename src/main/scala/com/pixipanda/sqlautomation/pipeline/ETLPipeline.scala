package com.pixipanda.sqlautomation.pipeline

import java.io.InputStreamReader

import com.pixipanda.sqlautomation.config.SQLAutomate
import com.pixipanda.sqlautomation.factory.HandlerFactory
import com.pixipanda.sqlautomation.handler.Handler
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}

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

  def buildPipeline(sqlAutomate: SQLAutomate, fileSystem: FileSystem): ETLPipeline = {

    val etlPipeline = ETLPipeline()

    sqlAutomate.sqlConfigs.foreach(sqlConfig => {
      val reader = new InputStreamReader(fileSystem.open(new Path(sqlConfig.fileName)))
      val config: Config = ConfigFactory.parseReader(reader)
      sqlConfig.queryConfigs.foreach(queryConfig => {
        val handlerFactory = HandlerFactory(queryConfig.etlType)
        val handler = handlerFactory.getHandler(config, queryConfig)
        etlPipeline.addHandlers(handler)
      })
    })
    etlPipeline
  }
}


