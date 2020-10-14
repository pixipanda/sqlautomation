package com.pixipanda.sqlautomation.writer

import com.pixipanda.sqlautomation.config.common.SinkConfig
import com.pixipanda.sqlautomation.constants.DataStores._
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
