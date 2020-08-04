package com.pixipanda.sqlautomation.config


import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger



object ConfigRegistry {

  val logger: Logger = Logger.getLogger(getClass.getName)

  private var _env: String = _

  def setEnv(env: String): Unit = _env = env

  def getEnv: String = _env

  private  var _config: Config = _

  def getConfig:Config = _config

  def parseConfig(file:String = null): Unit = {
    _config =  if (null != file) {
      ConfigFactory.parseFile(new File(file))
    }
    else
      ConfigFactory.load()
  }

  lazy val appConfig: AppConfig = AppConfig.parse(_config)


  def debugAppConfig(): Unit = {
    logger.debug(appConfig.extractConfig)
    logger.debug(appConfig.transformConfig)
    logger.debug(appConfig.loadConfig)

  }
}


