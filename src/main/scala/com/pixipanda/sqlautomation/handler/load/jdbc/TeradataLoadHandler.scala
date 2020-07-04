package com.pixipanda.sqlautomation.handler.load.jdbc

import com.pixipanda.sqlautomation.config.save.SaveConfig

case class TeradataLoadHandler(query: String, saveConfig: SaveConfig) extends JdbcLoadHandler(query, saveConfig){

  override def process(): Unit = super.process()
  override def decryptPassword: String = super.decryptPassword
}
