package com.pixipanda.sqlautomation.config

import org.scalatest.FunSpec

class AppConfigSpec extends  FunSpec{

  describe("Parse a simple config file")
  {
    val loadConfig = ConfigObject.loadConfig
    val extractConfig = ConfigObject.extractConfig

    describe("Parse Job1 config file") {

      val transformConfig = ConfigObject.job1TransformConfig
      val expected = AppConfig(Some(extractConfig), Some(transformConfig), loadConfig)

      it("should parse Extract, Transform and Load config") {
        val sut = Utils.buildConfig("src/test/resources/jobs/job1/job1.conf")
        assert(sut == expected)
      }

    }


    describe("Parse Job2 config file") {

      val expected = AppConfig(Some(extractConfig), None, loadConfig)

      it("should parse Job2 config file containing only Extract and Load config") {
        val sut = Utils.buildConfig("src/test/resources/jobs/job2/job2.conf")
        assert(sut == expected)

      }
    }


    describe("Parse Job3 config file") {

      val transormConfig = ConfigObject.job3TransformConfig
      val expected = AppConfig(None, None, loadConfig)

      it("should parse job3 config file containing only Transform and Load config") {
        val sut = Utils.buildConfig("src/test/resources/jobs/job3/job3.conf")
      }
    }
  }
}
