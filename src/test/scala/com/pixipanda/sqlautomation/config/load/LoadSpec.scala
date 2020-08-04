package com.pixipanda.sqlautomation.config.load

import com.pixipanda.sqlautomation.config.{ConfigObject, SinkConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class LoadSpec extends FunSpec {

  describe("Simple Load Parsing") {

    val hiveSinkConfig = ConfigObject.hiveSinkConfig
    val csvSinkConfig = ConfigObject.csvSinkConfig

   it("should parse simple load config string") {

      val configString =
        """
          |Load {
          |  sinks = [
          |    {
          |      sinkType = "hive"
          |      options {
          |        db = "hiveDb1"
          |        table = "hiveTable1"
          |        format = "orc"
          |        mode = "overwrite"
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      val expected = LoadConfig(List(hiveSinkConfig))
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
          |        db = "hiveDb1"
          |        table = "hiveTable1"
          |        format = "orc"
          |        mode = "overwrite"
          |      }
          |    },
          |    {
          |      sinkType = "csv"
          |      options {
          |        path = "/path/to/abc.csv"
          |        format = "csv"
          |        delimiter = "|"
          |        header = "true"
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      val expected = LoadConfig(List(hiveSinkConfig, csvSinkConfig))
      val config = ConfigFactory.parseString(configString)
      val sut = LoadConfig.parse(config)
      assert(sut == expected)
    }
  }
}
