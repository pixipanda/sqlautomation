package com.pixipanda.sqlautomation.config.etl.load

import com.pixipanda.sqlautomation.config.common.SinkConfig
import com.typesafe.config.Config

import scala.collection.JavaConverters._

case class LoadConfig(sinkConfigs: Seq[SinkConfig])

object LoadConfig {

  def parse(config: Config): Option[LoadConfig] = {
    if(config.hasPath("Load")) {
      val loadConfig = config.getConfig("Load")
      val sinkConfigs = loadConfig
        .getConfigList("sinks")
        .asScala
        .map(SinkConfig.parse)
        .toList
      Some(LoadConfig(sinkConfigs))
    } else None
  }
}
