package com.pixipanda.sqlautomation.config

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class SourceSpec extends FunSpec{

  describe("Simple Source Parsing") {

    it("should parse simple Source config String") {

      val configString =
        """{
          |  sourceType = "ftpServer"
          |    options {
          |      server = "abc.com"
          |      username = "user"
          |      password = ftpPassword
          |      privateKey = privateKey1
          |  }
          |}
        """.stripMargin

      val ftpOptions = Map(
        "server" -> "abc.com",
        "username" -> "user",
        "password" -> "ftpPassword",
        "privateKey" -> "privateKey"
      )
      val expectedResult = SourceConfig("ftpServer", None, ftpOptions)

      val config = ConfigFactory.parseString(configString)
      val sut = SourceConfig.parse(config)
      assert(sut == expectedResult)
    }
  }

}
