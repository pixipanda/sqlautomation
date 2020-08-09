package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.{ConfigRegistry, ConfigUtils}
import com.pixipanda.sqlautomation.reader.file.FileReader
import com.pixipanda.sqlautomation.reader.ftp.FtpReader
import org.scalatest.FunSpec

class ReaderFactorySpec extends FunSpec{

  describe("Reader Factory Spec") {

    describe("Reader Functionality") {

      it("should create csv Reader from csv_transform_hive.conf") {
        ConfigRegistry.parseConfig("src/test/resources/jobs/csv_transform_hive/csv_transform_hive.conf")
        val csvSourceConfig = ConfigRegistry.appConfig.extractConfig.get.sourceConfigs.head
        val expected = FileReader(csvSourceConfig)
        val sut = ReaderFactory.getReader(csvSourceConfig)
        assert(sut == expected)
      }

      it("should create ftp Reader from download_from_ftp.conf") {
        ConfigRegistry.parseConfig("src/test/resources/jobs/download_from_ftp/download_from_ftp.conf")
        val ftpSourceConfig = ConfigRegistry.appConfig.extractConfig.get.sourceConfigs.head
        val expected = FtpReader(ftpSourceConfig)
        val sut = ReaderFactory.getReader(ftpSourceConfig)
        assert(sut == expected)
      }
    }
  }
}
