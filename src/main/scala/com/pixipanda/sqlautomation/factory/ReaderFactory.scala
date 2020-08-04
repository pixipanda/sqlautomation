package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.SourceConfig
import com.pixipanda.sqlautomation.constants.Source._
import com.pixipanda.sqlautomation.reader.file.FileReader

object ReaderFactory {

  def getReader(sourceConfig: SourceConfig): FileReader = {

    sourceConfig.sourceType match {
      case CSV |
        XML |
        JSON |
        ORC |
        PARQUET |
        FTPSERVER  => FileReader(sourceConfig)

    }
  }
}
