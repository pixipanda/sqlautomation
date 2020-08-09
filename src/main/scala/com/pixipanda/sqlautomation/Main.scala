package com.pixipanda.sqlautomation

import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.pipeline.Pipeline


object Main extends Spark {

  def buildPipeline: Pipeline[Unit, _ >: Unit with DContainer] = {
    ConfigRegistry.parseConfig()
    ConfigRegistry.debugAppConfig()
    Pipeline.buildPipeline(ConfigRegistry.appConfig)
  }

  def runPipeline(): Unit = {
    buildPipeline.process()
  }

  def main(args: Array[String]): Unit = {
    runPipeline()
  }
}
