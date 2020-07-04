package com.pixipanda.sqlautomation.config


import com.pixipanda.sqlautomation.config.queryconfig.{LoadConfig, TransformConfig}
import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

object ConfigRegistry {

  var _env: String = _

  def apply(env: String): Unit = _env = env

  val logger: Logger = Logger.getLogger(getClass.getName)


  def config: Config = {
    val envConfig = ConfigFactory.parseResources(s"${_env}.conf")
    ConfigFactory.load()
      .withFallback(envConfig)
      .resolve()
  }

  lazy val sqlAutomate: SQLAutomate = SQLAutomate.parseSQLAutomate(config)


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

    logger.info("order: " + save.sourceType)
    logger.info("order: " + save.partition)
    logger.info("order: " + save.MSCKRepair)
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


