package com.pixipanda.sqlautomation.config.common

import com.pixipanda.sqlautomation.config.jdbc.JdbcConfig
import com.typesafe.config.Config
import org.apache.log4j.Logger

import scala.collection.JavaConverters._

case class SinkConfig(sinkType: String, repartition: Option[Seq[String]],options: Map[String, String])

object SinkConfig {

  val logger: Logger = Logger.getLogger(getClass.getName)

  private def parseRepartition(config: Config): Option[Seq[String]] = {
    if(config.hasPath("repartition")) Some(config.getStringList("repartition").asScala.toList) else None
  }

  def parse(config: Config): SinkConfig = {
    val sinkType =  config.getString("sinkType")
    val repartition = parseRepartition(config)
    val optionsConfig = config.getConfig("options")
    val options = Options.parse(optionsConfig)
    val jdbcOptions = JdbcConfig.getJdbcAsMap(sinkType)
    SinkConfig(sinkType, repartition, options ++ jdbcOptions)
  }
}
