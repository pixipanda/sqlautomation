package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.extract.ExtractConfig
import com.pixipanda.sqlautomation.config.load.LoadConfig
import com.pixipanda.sqlautomation.config.transform.TransformConfig
import com.typesafe.config.Config

case class AppConfig(extractConfig: Option[ExtractConfig], transformConfig: Option[TransformConfig], loadConfig: LoadConfig)

object AppConfig {

  def parse(config: Config): AppConfig = {
    val etlConfig = config.getConfig("ETL")
    val extractConfig = ExtractConfig.parse(etlConfig)
    val transformConfig = TransformConfig.parse(etlConfig)
    val loadConfig = LoadConfig.parse(etlConfig)
    AppConfig(extractConfig, transformConfig, loadConfig)
  }
}
