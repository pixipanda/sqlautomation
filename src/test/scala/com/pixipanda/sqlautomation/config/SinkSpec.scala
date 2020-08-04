package com.pixipanda.sqlautomation.config

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class SinkSpec extends FunSpec{

  describe("Simple Sink Parsing") {

    it("should parse simple Sink config String") {

      val configString =
        """{
          |  sinkType = "hive"
          |  options {
          |    db = "hiveDb1"
          |    table = "hiveTable1"
          |    format = "orc"
          |    mode = "overwrite"
          |  }
          |}
        """.stripMargin

      val hiveOptions = Map(
        "db" -> "hiveDb1",
        "table" -> "hiveTable1",
        "format" -> "orc",
        "mode" -> "overwrite"
      )
      val expectedResult = SinkConfig("hive", None, hiveOptions)

      val config = ConfigFactory.parseString(configString)
      val sut = SinkConfig.parse(config)
      assert(sut == expectedResult)
    }
  }
}
