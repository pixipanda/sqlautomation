package com.pixipanda.sqlautomation.config.jdbc

import java.util.Properties

import com.pixipanda.sqlautomation.config.ConfigRegistry
import org.scalatest.FunSpec

class JdbcConfigSpec extends FunSpec{

  describe("Jdbc Config Parser") {

    it("Should pass simple jdbc config file") {
      val emptyProps = new Properties()
      val teraDataJdbcConfig = JdbcConfig("teradata", "com.teradata.jdbc.TeraDriver", "teradata/qa/url", "teradata", "", emptyProps)
      val mySqlJdbcConfig = JdbcConfig("mysql", "com.mysql.jdbc.Driver", "mysql/qa/url", "mysql", "", emptyProps)
      val db2JdbcConfig = JdbcConfig("db2", "com.ibm.db2.jcc.DB2Driver", "db2/qa/url", "db2", "", emptyProps)
      val expectedJdbcConfigs: Map[String, JdbcConfig] = Map(
        "teradata" -> teraDataJdbcConfig,
        "mysql" -> mySqlJdbcConfig,
        "db2" -> db2JdbcConfig
      )

      ConfigRegistry.setEnv("qa")
      val sut = JdbcConfig.jdbcConfigs
      assert(sut == expectedJdbcConfigs)
    }
  }
}
