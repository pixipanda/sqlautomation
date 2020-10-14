package com.pixipanda.sqlautomation

import java.io.FileInputStream
import java.util.Properties

import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.pipeline.Pipeline
import org.apache.log4j.PropertyConfigurator
import org.slf4j.{Logger, LoggerFactory}


object Main extends Spark {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)


  def configLogging(path: String): Unit = {
    val props = new Properties()
    props.loadFromXML(new FileInputStream(path))
    PropertyConfigurator.configure(props)
    LOGGER.info("Logging configured!")
  }

  def buildPipeline: Pipeline[Unit, _ >: Unit with DContainer] = {

    LOGGER.info("Starting SQL Automation")
    ConfigRegistry.parseConfig()
    ConfigRegistry.debugAppConfig()
    Pipeline.buildPipeline(ConfigRegistry.appConfig)

  }

  def runPipeline(): Unit = {
    buildPipeline.process()
  }

  def main(args: Array[String]): Unit = {
    val loggerConfigFile = args(0)
    configLogging(loggerConfigFile)
    runPipeline()
  }
}
