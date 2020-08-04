package com.pixipanda.sqlautomation.config.extract

import com.pixipanda.sqlautomation.config.{ConfigObject, SourceConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class ExtractSpec extends FunSpec {

  describe("Simple Extract Parsing") {

    val ftpSourceConfig = ConfigObject.ftpSourceConfig
    val csvSourceConfig = ConfigObject.csvSourceConfig

    it("should parse simple extract config string") {

      val configString =
        """
          |Extract {
          |  sources = [
          |    {
          |      sourceType = "ftpServer"
          |      options {
          |        server = "abc.com"
          |        username = "user"
          |        password = ftpPassword
          |        privateKey = privateKey
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      val expected = Some(ExtractConfig(List(ftpSourceConfig)))
      val config = ConfigFactory.parseString(configString)
      val sut = ExtractConfig.parse(config)
      assert(sut == expected)
    }


    it("should parse multiple sources") {

      val configString =
        """
          |Extract {
          |  sources = [
          |    {
          |      sourceType = "ftpServer"
          |      options {
          |        server = "abc.com"
          |        username = "user"
          |        password = ftpPassword
          |        privateKey = privateKey
          |      }
          |    },
          |    {
          |      sourceType = "csv"
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

      val expected = Some(ExtractConfig(List(ftpSourceConfig, csvSourceConfig)))
      val config = ConfigFactory.parseString(configString)
      val sut = ExtractConfig.parse(config)
      assert(sut == expected)
    }
  }
}
