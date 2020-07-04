package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.queryconfig.{LoadConfig, TransformConfig}
import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.constants.ETL
import com.pixipanda.sqlautomation.TestingSparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class ConfigParserSpec extends FunSpec with BeforeAndAfterAll{

  override def beforeAll(): Unit = TestingSparkSession.configTestLog4j("OFF", "OFF")

  describe("ConfigParser") {

    val options = Map("db" -> "db1", "table" -> "table1", "format" -> "orc", "mode" -> "overwrite")
    val save = SaveConfig("hive", Some(List("colA", "colB")), Some(true), "path1", options)
    val queryConfig1 = TransformConfig(1, "q1", "q1view", ETL.TRANSFORM)
    val queryConfig2 = TransformConfig(2, "q2", "q2view", ETL.TRANSFORM)
    val queryConfig3 = TransformConfig(3, "q3", "q3view", ETL.TRANSFORM)
    val queryConfig4 = LoadConfig(4, "q4", save, ETL.LOAD)

    val sqlConfig1 = SQLConfig(1, "abc.conf", List(queryConfig1))
    val sqlConfig2 = SQLConfig(2, "def.conf", List(queryConfig1, queryConfig2, queryConfig3, queryConfig4))

    val expectedResult = SQLAutomate(List(sqlConfig1, sqlConfig2))

    it("should correctly parse simple conf file") {
      ConfigRegistry("qa")
      val sut = ConfigRegistry.sqlAutomate
      assert(sut == expectedResult)
    }
  }
}