package com.pixipanda.handler.load.jdbc

import com.pixipanda.sqlautomation.config.save.SaveConfig

case class DB2LoadHandler(query: String, saveConfig: SaveConfig) extends JdbcLoadHandler(query, saveConfig) {

  override def process(): Unit = super.process()
  override def decryptPassword: String = super.decryptPassword
}
