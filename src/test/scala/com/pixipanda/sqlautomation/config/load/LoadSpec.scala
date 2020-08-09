package com.pixipanda.sqlautomation.config.load

import com.pixipanda.sqlautomation.config.ConfigUtils
import com.pixipanda.sqlautomation.config.etl.load.LoadConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class LoadSpec extends FunSpec {

  describe("Simple Load Parsing") {

    val hiveSinkConfig = ConfigUtils.hiveSinkConfig
    val csvSinkConfig = ConfigUtils.csvSinkConfig

    it("should parse simple load config string") {

      val configString =
        """
          |Load {
          |  sinks = [
          |    {
          |      sinkType = "hive"
          |      options {
          |        db = "test_db1"
          |        table = "employee"
          |        format = "orc"
          |        mode = "overwrite"
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      val expected = Some(LoadConfig(List(hiveSinkConfig)))
      val config = ConfigFactory.parseString(configString)
      val sut = LoadConfig.parse(config)
      assert(sut == expected)
    }


    it("should parse multiple sinks") {

      val configString =
        """
          |Load {
          |  sinks = [
          |    {
          |      sinkType = "hive"
          |      options {
          |        db = "test_db1"
          |        table = "employee"
          |        format = "orc"
          |        mode = "overwrite"
          |      }
          |    },
          |    {
          |      sinkType = "csv"
          |      options {
          |        path = "/tmp/csvfiles/output"
          |        format = "csv"
          |        header = "true"
          |        mode = "overwrite"
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      val expected = Some(LoadConfig(List(hiveSinkConfig, csvSinkConfig)))
      val config = ConfigFactory.parseString(configString)
      val sut = LoadConfig.parse(config)
      assert(sut == expected)
    }
  }
}