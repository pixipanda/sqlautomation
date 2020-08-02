package com.pixipanda.sqlautomation.config


import java.io.File

import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.sqlautomation.config.queryconfig.{LoadConfig, TransformConfig}
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

  lazy val sqlAutomate: SQLAutomate = SQLAutomate.parseSQLAutomate(_config)


  def debugTransformConfig(transformConfig: TransformConfig): Unit = {
    logger.info("order: " + transformConfig.order)
    logger.info("queryName: " + transformConfig.query)
    logger.info("viewName: " + transformConfig.query)
  }


  def debugLoadConfig(loadConfig: LoadConfig): Unit = {
    logger.info("order: " + loadConfig.order)
    logger.info("queryName: " + loadConfig.query)
    debugSaveConfig(loadConfig.save)
  }


  def debugSaveConfig(save: SaveConfig): Unit = {

    logger.info("sourceType: " + save.sourceType)
    logger.info("repartition: " + save.repartition)
    logger.info("options: " + save.options)
  }


  def debugConfig(): Unit = {

    sqlAutomate.sqlConfigs.foreach(sqlConf => {
      logger.info("order" + sqlConf.order)
      logger.info("fileName" + sqlConf.fileName)
      sqlConf.queryConfigs.foreach({
        case queryConfig: TransformConfig => debugTransformConfig(queryConfig)
        case queryConfig: LoadConfig => debugLoadConfig(queryConfig)
      })
    })
  }
}


