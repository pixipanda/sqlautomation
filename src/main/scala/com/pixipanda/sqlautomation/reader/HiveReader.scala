package com.pixipanda.sqlautomation.reader

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.config.common.SourceConfig
import com.pixipanda.sqlautomation.container.DContainer

case class HiveReader(sourceConfig: SourceConfig) extends Reader with Spark {

  override def read: DContainer = ???
}
