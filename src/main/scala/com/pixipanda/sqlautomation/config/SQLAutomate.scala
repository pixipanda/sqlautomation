package com.pixipanda.sqlautomation.config

import com.typesafe.config.Config

case class SQLAutomate(sqlConfigs: Seq[SQLConfig]) {

}

object SQLAutomate {
  def parseSQLAutomate(config: Config): SQLAutomate = {

    if (config.hasPath("SQLAutomate")) {
      val cfg = config.getConfig("SQLAutomate")
      val sql = SQLConfig.parseSQLConfig(cfg)
      SQLAutomate(sql)
    } else {
      SQLAutomate(null)
    }
  }
}
