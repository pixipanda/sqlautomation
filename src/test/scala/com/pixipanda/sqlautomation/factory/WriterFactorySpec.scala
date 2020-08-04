package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.ConfigObject
import com.pixipanda.sqlautomation.writer.HiveWriter
import com.pixipanda.sqlautomation.writer.file.FileWriter
import org.scalatest.FunSpec

class WriterFactorySpec extends FunSpec{

  describe("Writer Factory Spec") {

    describe("Functionality") {

      it("should create csv Writer") {

        val csvSinkConfig = ConfigObject.csvSinkConfig
        val csvWriter = FileWriter(csvSinkConfig)
        val sut = WriterFactory.getWriter(csvSinkConfig)
        assert(sut == csvWriter)
      }

      it("should create hive Writer") {

        val hiveSinkConfig = ConfigObject.hiveSinkConfig
        val hiveWriter = HiveWriter(hiveSinkConfig)
        val sut = WriterFactory.getWriter(hiveSinkConfig)
        assert(sut == hiveWriter)
      }
    }
  }
}
