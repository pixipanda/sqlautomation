/*
package com.pixipanda.sqlautomation.handler.load

import com.pixipanda.sqlautomation.config.queryconfig.LoadConfig
import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.sqlautomation.constants.ETL
import com.pixipanda.sqlautomation.handler.load.jdbc.TeradataLoadHandler
import org.scalatest.FunSpec

class LoadHandlerSpec extends FunSpec{

  describe("Hive Load Handler") {


    describe("Hive Load Handler with orc format") {

      val hiveOrcOptions = Map("db" -> "db1", "table" -> "table1", "format" -> "orc", "mode" -> "overwrite")
      val hiveSaveConfig = SaveConfig("hive", Some(List("colA", "colB")), hiveOrcOptions)
      val queryConfig = LoadConfig(4, "SELECT * FROM db.table", hiveSaveConfig, ETL.LOAD)

      val hiveLoadHandler = HiveLoadHandler("SELECT * FROM db.table", hiveSaveConfig)

      it("should create Hive Load Handler with orc format") {
        val loadHandlerFactory = LoadHandlerFactory()
        val sut =loadHandlerFactory.getHandler(queryConfig)
        assert(sut == hiveLoadHandler)
      }
    }
  }


  describe("Teradata Load Handler") {

    val teraDataOptions = Map("db" -> "db1", "table" -> "table1", "mode" -> "overwrite")
    val teraDataJdbcOptions = Map("driver" -> "com.teradata.jdbc.TeraDriver", "url" -> "teradata/qa/url", "username" -> "teradata", "password" -> "")
    val teraDataSaveConfig = SaveConfig("teradata", None, teraDataOptions ++ teraDataJdbcOptions)
    val queryConfig = LoadConfig(4, "SELECT * FROM db.table", teraDataSaveConfig, ETL.LOAD)

    val teraDataLoadHandler = TeradataLoadHandler("SELECT * FROM db.table", teraDataSaveConfig)

    it("should create Teradata Load Handler with Jdbc options") {

      val loadHandlerFactory = LoadHandlerFactory()
      val sut =loadHandlerFactory.getHandler(queryConfig)
      assert(sut == teraDataLoadHandler)

    }
  }
}
*/
