package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.common.SinkConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class SinkSpec extends FunSpec{

  describe("Simple Sink Parsing") {

    it("should parse simple Sink config String") {

      val configString =
        """{
          |  sinkType = "hive"
          |  options {
          |    db = "test_db1"
          |    table = "employee"
          |    format = "orc"
          |    mode = "overwrite"
          |  }
          |}
        """.stripMargin

      val expected = ConfigUtils.hiveSinkConfig

      val config = ConfigFactory.parseString(configString)
      val sut = SinkConfig.parse(config)
      assert(sut == expected)
    }
  }
}
