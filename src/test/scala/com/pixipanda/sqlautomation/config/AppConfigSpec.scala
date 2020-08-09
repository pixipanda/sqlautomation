package com.pixipanda.sqlautomation.config


import com.pixipanda.sqlautomation.config.etl.extract.ExtractConfig
import org.scalatest.FunSpec

class AppConfigSpec extends  FunSpec{

  describe("Parse a simple config file"){
    val loadConfig = ConfigUtils.loadConfig
    val extractConfig = ExtractConfig(List(ConfigUtils.csvSourceViewConfig))

    describe("Parse csv_transform_hive config file") {

      val transformConfig = ConfigUtils.job1TransformConfig
      val expected = AppConfig(Some(extractConfig), Some(transformConfig), Some(loadConfig))

      it("should parse Extract, Transform and Load config") {
        val sut = ConfigUtils.buildConfig("src/test/resources/jobs/csv_transform_hive/csv_transform_hive.conf")
        println("sut: " + sut)
        assert(sut == expected)
      }

    }


    describe("Parse csv_to_hive config file") {

      val extractConfig = ExtractConfig(List(ConfigUtils.csvSourceConfig))
      val expected = AppConfig(Some(extractConfig), None, Some(loadConfig))

      it("should parse csv_to_hive config file containing only Extract and Load config") {
        val sut = ConfigUtils.buildConfig("src/test/resources/jobs/csv_to_hive/csv_to_hive.conf")
        assert(sut == expected)

      }
    }


    describe("Parse hive_to_csv config file") {

      val transormConfig = ConfigUtils.job3TransformConfig
      val expected = AppConfig(None, None, Some(loadConfig))

      it("should parse hive_to_csv config file containing only Transform and Load config") {
        val sut = ConfigUtils.buildConfig("src/test/resources/jobs/hive_to_csv/hive_to_csv.conf")
      }
    }
  }
}
