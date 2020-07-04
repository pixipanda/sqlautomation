package com.pixipanda.sqlautomation.config.queryconfig

import com.pixipanda.sqlautomation.constants.ETL.LOAD
import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.typesafe.config.Config




case class LoadConfig (override val order: Int, queryName: String, save: SaveConfig, override val etlType: String) extends QueryConfig(order, etlType)

object LoadConfig {

  def parseLoadConfig(config: Config): Option[LoadConfig] = {

    if(config.hasPath("save")) {
      val order = config.getInt("order")
      val query = config.getString("queryName")
      val save = SaveConfig.parseSaveConfig(config)
      Some(LoadConfig(order, query, save, LOAD))
    } else None

  }
}
