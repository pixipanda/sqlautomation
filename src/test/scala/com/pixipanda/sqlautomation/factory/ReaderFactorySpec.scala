package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.ConfigObject
import com.pixipanda.sqlautomation.reader.file.FileReader
import org.scalatest.FunSpec

class ReaderFactorySpec extends FunSpec{

  describe("Reader Factory Spec") {

    describe("Functionality") {

      it("should create csv Reader") {

        val csvSourceConfig = ConfigObject.csvSourceConfig
        val csvReader = FileReader(csvSourceConfig)
        val sut = ReaderFactory.getReader(csvSourceConfig)
        assert(sut == csvReader)
      }

      it("should create ftp Reader") {
        val ftpSourceConfig = ConfigObject.ftpSourceConfig
        val ftpReader = FileReader(ftpSourceConfig)
        val sut = ReaderFactory.getReader(ftpSourceConfig)
        assert(sut == ftpReader)
      }
    }
  }
}
