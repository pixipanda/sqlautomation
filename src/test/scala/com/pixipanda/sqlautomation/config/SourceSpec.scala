package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.common.SourceConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class SourceSpec extends FunSpec{

  describe("Simple Source Parsing") {

    it("should parse simple Source config String") {

      val configString =
        """{
          |  sourceType = "ftpServer"
          |  options {
          |    host = "pixipanda"
          |    username = "hduser"
          |    privateKey = "C:\\workspace\\Engineering\\passwordless\\private_key.ppk"
          |    tempLocation = "C:\\tmp\\local_tmp"
          |    hdfsTempLocation = "/tmp/hdfs_tmp"
          |    path = "/tmp/csvfiles/input/employee.csv"
          |  }
          |}
        """.stripMargin

      val expected = ConfigUtils.ftpSourceConfig

      val config = ConfigFactory.parseString(configString)
      val sut = SourceConfig.parse(config)
      assert(sut == expected)
    }
  }
}
