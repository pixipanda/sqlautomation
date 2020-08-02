package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.queryconfig.{LoadConfig, TransformConfig}
import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.sqlautomation.constants.ETL
import org.scalatest.FunSpec

class ConfigRegistrySpec extends FunSpec {


  describe("ConfigParser") {

    val hiveOrcOptions = Map("db" -> "db1", "table" -> "table1", "format" -> "orc", "mode" -> "overwrite")


    val queryConfig1 = TransformConfig(1, "SELECT * FROM db.table", "view11", ETL.TRANSFORM)
    val queryConfig2 = TransformConfig(2, "SELECT * FROM view11", "view12", ETL.TRANSFORM)
    val queryConfig3 = TransformConfig(1, "SELECT * FROM view11 join view12", "view21", ETL.TRANSFORM)

    val sqlConfig1 = SQLConfig(1, "sqlFile1.conf", List(queryConfig1, queryConfig2))


    it("should correctly parse simple hive conf file") {
      val hiveSaveConfig = SaveConfig("hive", Some(List("colA", "colB")), hiveOrcOptions)
      val queryConfig4 = LoadConfig(2, "SELECT * FROM view11 join view12 join view21", hiveSaveConfig, ETL.LOAD)
      val sqlConfig2 = SQLConfig(2, "sqlFile2.conf", List(queryConfig3, queryConfig4))
      val hiveExpectedResult = SQLAutomate(List(sqlConfig1, sqlConfig2))
      ConfigRegistry.setEnv("qa")
      ConfigRegistry.parseConfig("src/test/resources/hive_load.conf")
      val sut = SQLAutomate.parseSQLAutomate(ConfigRegistry.getConfig)
      assert(sut == hiveExpectedResult)
    }


    it("should correctly parse simple teradata conf file with Jdbc") {
      val teraDataOptions = Map("db" -> "db1", "table" -> "table1", "mode" -> "overwrite")
      val teraDataJdbcOptions = Map("driver" -> "com.teradata.jdbc.TeraDriver", "url" -> "teradata/qa/url", "username" -> "teradata", "password" -> "")
      val teraDataSaveConfig = SaveConfig("teradata", None, teraDataOptions ++ teraDataJdbcOptions)
      val queryConfig4 = LoadConfig(2, "SELECT * FROM view11 join view12 join view21", teraDataSaveConfig, ETL.LOAD)
      val sqlConfig2 = SQLConfig(2, "sqlFile2.conf", List(queryConfig3, queryConfig4))
      val terDataExpectedResult = SQLAutomate(List(sqlConfig1, sqlConfig2))
      ConfigRegistry.setEnv("qa")
      ConfigRegistry.parseConfig("src/test/resources/teradata_load.conf")
      val sut = SQLAutomate.parseSQLAutomate(ConfigRegistry.getConfig)
      assert(sut == terDataExpectedResult)
    }
  }
}