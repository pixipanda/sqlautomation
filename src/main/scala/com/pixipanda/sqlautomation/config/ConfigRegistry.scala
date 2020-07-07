package com.pixipanda.sqlautomation.config


import java.io.File

import com.pixipanda.sqlautomation.config.queryconfig.{LoadConfig, TransformConfig}
import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger



object ConfigRegistry {

  val logger: Logger = Logger.getLogger(getClass.getName)

  var _env: String = _

  def apply(env: String): Unit = _env = env


  def setEnv(env: String): Unit = _env = env

  def getEnv: String = _env

  def config(file:String = null): Config = {
    if(null != file)
      ConfigFactory.parseFile(new File(file))
    else
    ConfigFactory.load()
  }


  lazy val sqlAutomate: SQLAutomate = SQLAutomate.parseSQLAutomate(config())


  def debugTransformConfig(transformConfig: TransformConfig): Unit = {
    logger.info("order: " + transformConfig.order)
    logger.info("queryName: " + transformConfig.queryName)
    logger.info("viewName: " + transformConfig.viewName)
  }


  def debugLoadConfig(loadConfig: LoadConfig): Unit = {
    logger.info("order: " + loadConfig.order)
    logger.info("queryName: " + loadConfig.queryName)
    debugSaveConfig(loadConfig.save)
  }


  def debugSaveConfig(save: SaveConfig): Unit = {

    logger.info("sourceType: " + save.sourceType)
    logger.info("partition: " + save.partition)
    logger.info("MSCKRepair: " + save.MSCKRepair)
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


