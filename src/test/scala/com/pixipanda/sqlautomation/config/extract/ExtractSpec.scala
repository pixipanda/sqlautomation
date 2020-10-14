package com.pixipanda.sqlautomation.config.extract

import com.pixipanda.sqlautomation.config.ConfigUtils
import com.pixipanda.sqlautomation.config.etl.extract.ExtractConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class ExtractSpec extends FunSpec {

  describe("Simple Extract Parsing") {

    val ftpSourceConfig = ConfigUtils.ftpSourceConfig
    val csvSourceConfig = ConfigUtils.csvSourceConfig

    it("should parse simple extract config string") {

      val configString =
        """
          |Extract {
          |  sources = [
          |    {
          |      sourceType = "ftpServer"
          |      options {
          |        host = "pixipanda"
          |        username = "hduser"
          |        privateKey = "C:\\workspace\\Engineering\\passwordless\\private_key.ppk"
          |        tempLocation = "C:\\tmp\\local_tmp"
          |        hdfsTempLocation = "/tmp/hdfs_tmp"
          |        path = "/tmp/csvfiles/input/employee.csv"
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
          |        host = "pixipanda"
          |        username = "hduser"
          |        privateKey = "C:\\workspace\\Engineering\\passwordless\\private_key.ppk"
          |        tempLocation = "C:\\tmp\\local_tmp"
          |        hdfsTempLocation = "/tmp/hdfs_tmp"
          |        path = "/tmp/csvfiles/input/employee.csv"
          |      }
          |    },
          |    {
          |      sourceType = "csv"
          |      options {
          |        path = "/home/hduser/data/india/wf/employee/employee.csv"
          |        format = "csv"
          |        inferSchema = "true"
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