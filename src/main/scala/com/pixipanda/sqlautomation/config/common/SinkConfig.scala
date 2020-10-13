package com.pixipanda.sqlautomation.config.common

import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

case class SinkConfig(sinkType: String, repartition: Option[Seq[String]],options: Map[String, String])

object SinkConfig {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  private def parseRepartition(config: Config): Option[Seq[String]] = {
    if(config.hasPath("repartition")) Some(config.getStringList("repartition").asScala.toList) else None
  }

  def parse(config: Config): SinkConfig = {
    LOGGER.info(s"Parsing Sink Config")
    val sinkType =  config.getString("sinkType")
    val repartition = parseRepartition(config)
    val optionsConfig = config.getConfig("options")
    val options = Options.parse(optionsConfig)
    val sinkConfig = SinkConfig(sinkType, repartition, options)
    LOGGER.info(s"sinkConfig: $sinkConfig")
    sinkConfig
  }
}
