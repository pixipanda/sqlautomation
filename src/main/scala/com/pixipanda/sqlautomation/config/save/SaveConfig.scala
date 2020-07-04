package com.pixipanda.sqlautomation.config.save

import com.pixipanda.sqlautomation.config.jdbc.JdbcConfig
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.mutable

case class SaveConfig(
  sourceType: String,
  partition: Option[Seq[String]],
  MSCKRepair: Option[Boolean],
  path: String,
  options: Map[String, String]
)

object SaveConfig {

  private def parserPartition(config: Config): Option[Seq[String]] = {
    if(config.hasPath("partition")) Some(config.getStringList("partition").asScala.toList) else None
  }


  private def parserMSCKRepair(config: Config): Option[Boolean] = {
    if(config.hasPath("msckRepair")) Some(config.getBoolean("msckRepair")) else None
  }


  private def getOptions(config: Config): Map[String, String] = {

    val options = mutable.Map[String, String]()

    val sourceType =  config.getString("sourceType")
    val jdbcOptions = JdbcConfig.getJdbcAsMap(sourceType)

    options.put("db", config.getString("db"))
    options.put("table", config.getString("table"))
    options.put("format", config.getString("format"))
    options.put("mode", config.getString("mode"))

    options.toMap ++ jdbcOptions
  }


  def parseSaveConfig(config: Config): SaveConfig = {

    val saveConfig = config.getConfig("save")
    val sourceType =  saveConfig.getString("sourceType")
    val partition = parserPartition(saveConfig)
    val msckRepair = parserMSCKRepair(saveConfig)
    val path = saveConfig.getString("path")
    val options = getOptions(saveConfig)

    SaveConfig(sourceType, partition, msckRepair, path, options)
  }
}
