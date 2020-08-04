package com.pixipanda.sqlautomation.config

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class OptionsSpec extends FunSpec{

  describe("Simple options parsing") {

    it("should parse simple options config string") {

      val configString =
        """
          |options {
          |  db = "mysqlDb1"
          |  table = "mysqlTable1"
          |  username = "user"
          |  password = mysqlPassword
          |}
        """.stripMargin

      val expected = Map(
        "db" -> "mysqlDb1",
        "table" -> "mysqlTable1",
        "username" -> "user",
        "password" -> "mysqlPassword"
      )

      val config = ConfigFactory.parseString(configString)
      val optionConfig = config.getConfig("options")
      val sut = Options.parse(optionConfig)
      assert(sut == expected)
    }
  }
}
