package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.common.SourceConfig
import com.pixipanda.sqlautomation.constants.DataStores._
import com.pixipanda.sqlautomation.reader.Reader
import com.pixipanda.sqlautomation.reader.file.FileReader
import com.pixipanda.sqlautomation.reader.ftp.FtpReader

object ReaderFactory {

  def getReader(sourceConfig: SourceConfig): Reader = {

    sourceConfig.sourceType match {
      case CSV |
           XML |
           JSON |
           ORC |
           PARQUET => FileReader(sourceConfig)
      case FTPSERVER => FtpReader(sourceConfig)

    }
  }
}