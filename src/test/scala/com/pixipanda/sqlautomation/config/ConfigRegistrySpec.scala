package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.queryconfig.{LoadConfig, TransformConfig}
import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.sqlautomation.constants.ETL
import org.scalatest.FunSpec

class ConfigRegistrySpec extends FunSpec {


  describe("ConfigParser") {

    val hiveOrcOptions = Map("db" -> "db1", "table" -> "table1", "format" -> "orc", "mode" -> "overwrite")


    val queryConfig1 = TransformConfig(1, "q1", "q1view", ETL.TRANSFORM)
    val queryConfig2 = TransformConfig(2, "q2", "q2view", ETL.TRANSFORM)
    val queryConfig3 = TransformConfig(3, "q3", "q3view", ETL.TRANSFORM)



    val sqlConfig1 = SQLConfig(1, "abc.conf", List(queryConfig1))


    it("should correctly parse simple hive conf file") {
      val hiveSaveConfig = SaveConfig("hive", Some(List("colA", "colB")), Some(true), "path1", hiveOrcOptions)
      val queryConfig4 = LoadConfig(4, "q4", hiveSaveConfig, ETL.LOAD)
      val sqlConfig2 = SQLConfig(2, "def.conf", List(queryConfig1, queryConfig2, queryConfig3, queryConfig4))
      val hiveExpectedResult = SQLAutomate(List(sqlConfig1, sqlConfig2))
      ConfigRegistry.setEnv("qa")
      val sut = SQLAutomate.parseSQLAutomate(ConfigRegistry.config("src/test/resources/hive_load.conf"))
      assert(sut == hiveExpectedResult)
    }


    it("should correctly parse simple teradata conf file with Jdbc") {
      val teraDataOptions = Map("db" -> "db1", "table" -> "table1", "mode" -> "overwrite")
      val teraDataJdbcOptions = Map("driver" -> "com.teradata.jdbc.TeraDriver", "url" -> "teradata/qa/url", "username" -> "teradata", "password" -> "")
      val teraDataSaveConfig = SaveConfig("teradata", None, None, "path1", teraDataOptions ++ teraDataJdbcOptions)
      val queryConfig4 = LoadConfig(4, "q4", teraDataSaveConfig, ETL.LOAD)
      val sqlConfig2 = SQLConfig(2, "def.conf", List(queryConfig1, queryConfig2, queryConfig3, queryConfig4))
      val terDataExpectedResult = SQLAutomate(List(sqlConfig1, sqlConfig2))
      ConfigRegistry.setEnv("qa")
      val sut = SQLAutomate.parseSQLAutomate(ConfigRegistry.config("src/test/resources/teradata_load.conf"))
      assert(sut == terDataExpectedResult)
    }
  }
}