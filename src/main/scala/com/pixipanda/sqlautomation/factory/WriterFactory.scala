package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.SinkConfig
import com.pixipanda.sqlautomation.constants.Source._
import com.pixipanda.sqlautomation.writer.{HiveWriter, Writer}
import com.pixipanda.sqlautomation.writer.file.FileWriter

object WriterFactory {

  def getWriter(sinkConfig: SinkConfig):Writer = {
    sinkConfig.sinkType match {
      case CSV |
           XML |
           JSON |
           ORC |
           PARQUET |
           FTPSERVER  => FileWriter(sinkConfig)
      case HIVE => HiveWriter(sinkConfig)

    }
  }
}
