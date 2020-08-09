package com.pixipanda.sqlautomation.config.etl.transform

import com.typesafe.config.Config

import scala.collection.JavaConverters._

case class TransformConfig(sqlConfigs: Seq[SQLConfig])

object TransformConfig {

  def parse(config: Config):Option[TransformConfig] = {
    if(config.hasPath("Transform")) {
      val transformConfig = config.getConfig("Transform")
      val sqlConfig = transformConfig.getConfigList("sqls").asScala.map(SQLConfig.parse).toList.sortBy(_.order)
      Some(TransformConfig(sqlConfig))
    } else  None
  }
}