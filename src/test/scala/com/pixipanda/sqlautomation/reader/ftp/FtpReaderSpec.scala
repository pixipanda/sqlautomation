package com.pixipanda.sqlautomation.reader.ftp

import com.pixipanda.sqlautomation.config.ConfigUtils
import org.scalatest.FunSpec

class FtpReaderSpec extends FunSpec{

  describe("FTP Reader Spec") {

    it("should read file from ftp server") {

      val ftpSourceConfig = ConfigUtils.ftpSourceConfig
      val ftpReader = FtpReader(ftpSourceConfig)
      ftpReader.read
    }
  }
}
