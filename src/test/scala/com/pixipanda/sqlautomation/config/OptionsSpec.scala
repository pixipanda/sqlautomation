package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.common.Options
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class OptionsSpec extends FunSpec{

  describe("Simple options parsing") {

    it("should parse simple options config string") {

      val configString =
        """
          |options {
          |  db = "test_db1"
          |  table = "employee"
          |  format = "orc"
          |}
        """.stripMargin

      val expected = ConfigUtils.hiveSourceOptions

      val config = ConfigFactory.parseString(configString)
      val optionConfig = config.getConfig("options")
      val sut = Options.parse(optionConfig)
      assert(sut == expected)
    }
  }
}
