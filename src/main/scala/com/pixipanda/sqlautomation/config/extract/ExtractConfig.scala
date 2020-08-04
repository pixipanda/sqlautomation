package com.pixipanda.sqlautomation.config.extract

import com.pixipanda.sqlautomation.config.{ETLConfig, SourceConfig}
import com.typesafe.config.Config

import scala.collection.JavaConverters._

case class ExtractConfig(sourceConfigs: Seq[SourceConfig]) extends ETLConfig

object ExtractConfig {

  def parse(config: Config):Option[ExtractConfig] = {

    if(config.hasPath("Extract")) {
      val extractConfig = config.getConfig("Extract")
      val sourceConfigs = extractConfig
        .getConfigList("sources")
        .asScala
        .map(SourceConfig.parse)
        .toList
      Some(ExtractConfig(sourceConfigs))
    } else {
      None
    }
  }
}
