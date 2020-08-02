package com.pixipanda.sqlautomation.config

import com.typesafe.config.Config

import scala.collection.JavaConverters._

object Options {

  def parse(config: Config):Map[String, String] = {
    config.root
      .keySet
      .asScala
      .map(key => key -> config.getString(key))
      .toMap
  }
}
