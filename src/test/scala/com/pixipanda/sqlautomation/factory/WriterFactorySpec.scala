package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.writer.HiveWriter
import com.pixipanda.sqlautomation.writer.file.FileWriter
import org.scalatest.FunSpec

class WriterFactorySpec extends FunSpec{

  describe("Writer Factory Spec") {

    describe("Writer Functionality") {

      it("should create hive Writer from csv_to_hive") {
        ConfigRegistry.parseConfig("src/test/resources/jobs/csv_to_hive/csv_to_hive.conf")
        val hiveSinkConfig = ConfigRegistry.appConfig.loadConfig.get.sinkConfigs.head
        val expected = HiveWriter(hiveSinkConfig)
        val sut = WriterFactory.getWriter(hiveSinkConfig)
        assert(sut == expected)
      }

      it("should create csv Writer from hive_to_csv.conf") {
        ConfigRegistry.parseConfig("src/test/resources/jobs/hive_to_csv/hive_to_csv.conf")
        val csvSinkConfig = ConfigRegistry.appConfig.loadConfig.get.sinkConfigs.head
        val expected = FileWriter(csvSinkConfig)
        val sut = WriterFactory.getWriter(csvSinkConfig)
        assert(sut == expected)
      }
    }
  }
}
