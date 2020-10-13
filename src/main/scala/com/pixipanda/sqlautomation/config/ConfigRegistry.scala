package com.pixipanda.sqlautomation.config


import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}




object ConfigRegistry {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  private var _env: String = _

  def setEnv(env: String): Unit = _env = env

  def getEnv: String = _env

  private  var _config: Config = _

  def getConfig:Config = _config

  def parseConfig(file:String = null): Unit = {
    _config =  if (null != file) {
      ConfigFactory.parseFile(new File(file)).resolve()
    }
    else
      ConfigFactory.load().resolve()
  }

  lazy val appConfig: AppConfig = AppConfig.parse(_config)

  def clear(): Unit = _config = null

  def debugAppConfig(): Unit = {
    LOGGER.debug(s"Extract Config: ${appConfig.extractConfig}")
    LOGGER.debug(s"Transform Config: ${appConfig.transformConfig}")
    LOGGER.debug(s"Load Config: ${appConfig.loadConfig}")

  }
}


