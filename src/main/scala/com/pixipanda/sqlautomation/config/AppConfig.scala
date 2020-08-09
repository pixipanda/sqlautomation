package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.etl.extract.ExtractConfig
import com.pixipanda.sqlautomation.config.etl.load.LoadConfig
import com.pixipanda.sqlautomation.config.etl.transform.TransformConfig
import com.typesafe.config.Config

case class AppConfig(extractConfig: Option[ExtractConfig], transformConfig: Option[TransformConfig], loadConfig: Option[LoadConfig])

object AppConfig {

  def parse(config: Config): AppConfig = {
    val etlConfig = config.getConfig("ETL")
    val extractConfig = ExtractConfig.parse(etlConfig)
    val transformConfig = TransformConfig.parse(etlConfig)
    val loadConfig = LoadConfig.parse(etlConfig)
    AppConfig(extractConfig, transformConfig, loadConfig)
  }
}