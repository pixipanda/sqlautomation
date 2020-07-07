package com.pixipanda.sqlautomation.handler.load

import com.pixipanda.sqlautomation.config.queryconfig.LoadConfig
import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.sqlautomation.constants.ETL
import com.pixipanda.sqlautomation.factory.LoadHandlerFactory
import com.pixipanda.sqlautomation.handler.load.jdbc.TeradataLoadHandler
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class LoadHandlerSpec extends FunSpec{

  describe("Hive Load Handler") {


    describe("Hive Load Handler with orc format") {

     val queriesConfigString =
        """
          | {
          |   q1 = "SELECT * from db.table"
          | }
        """.stripMargin

      val hiveOrcOptions = Map("db" -> "db1", "table" -> "table1", "format" -> "orc", "mode" -> "overwrite")
      val hiveSaveConfig = SaveConfig("hive", Some(List("colA", "colB")), Some(true), "path1", hiveOrcOptions)
      val queryConfig = LoadConfig(4, "q1", hiveSaveConfig, ETL.LOAD)

      val hiveLoadHandler = HiveLoadHandler("SELECT * from db.table", hiveSaveConfig)

      it("should create Hive Load Handler with orc format") {

        val config = ConfigFactory.parseString(queriesConfigString)
        val loadHandlerFactory = LoadHandlerFactory()
        val sut =loadHandlerFactory.getHandler(config, queryConfig)
        assert(sut == hiveLoadHandler)
      }
    }
  }


  describe("Teradata Load Handler") {

    val queriesConfigString =
      """
        | {
        |   q1 = "SELECT * from db.table"
        | }
      """.stripMargin

    val teraDataOptions = Map("db" -> "db1", "table" -> "table1", "mode" -> "overwrite")
    val teraDataJdbcOptions = Map("driver" -> "com.teradata.jdbc.TeraDriver", "url" -> "teradata/qa/url", "username" -> "teradata", "password" -> "")
    val teraDataSaveConfig = SaveConfig("teradata", None, None, "path1", teraDataOptions ++ teraDataJdbcOptions)
    val queryConfig = LoadConfig(4, "q1", teraDataSaveConfig, ETL.LOAD)

    val teraDataLoadHandler = TeradataLoadHandler("SELECT * from db.table", teraDataSaveConfig)

    it("should create Teradata Load Handler with Jdbc options") {

      val config = ConfigFactory.parseString(queriesConfigString)
      val loadHandlerFactory = LoadHandlerFactory()
      val sut =loadHandlerFactory.getHandler(config, queryConfig)
      assert(sut == teraDataLoadHandler)

    }
  }
}
