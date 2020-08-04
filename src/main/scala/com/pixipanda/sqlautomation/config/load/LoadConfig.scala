package com.pixipanda.sqlautomation.config.load

import com.pixipanda.sqlautomation.config.SinkConfig
import com.typesafe.config.Config

import scala.collection.JavaConverters._

case class LoadConfig(sinkConfigs: Seq[SinkConfig])

object LoadConfig {

  def parse(config: Config):LoadConfig = {

    val loadConfig = config.getConfig("Load")
    val sinkConfigs = loadConfig
      .getConfigList("sinks")
      .asScala
      .map(SinkConfig.parse)
      .toList
    LoadConfig(sinkConfigs)

  }
}
