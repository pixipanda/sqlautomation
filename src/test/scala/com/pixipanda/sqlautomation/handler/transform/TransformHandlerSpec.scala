package com.pixipanda.sqlautomation.handler.transform

import com.pixipanda.sqlautomation.config.queryconfig.TransformConfig
import com.pixipanda.sqlautomation.constants.ETL
import com.pixipanda.sqlautomation.factory.TransformHandlerFactory
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class TransformHandlerSpec extends FunSpec{

  describe("Spark Transform Handler") {

    val configString =
      """
        | {
        |   q1 = "SELECT * from db.table"
        | }
      """.stripMargin


    val queryConfig = TransformConfig(1, "q1", "q1view", ETL.TRANSFORM)
    val sparkHandler = SparkTransformHandler("SELECT * from db.table", "q1view")

    it("should create Spark Load Handler") {
      val config = ConfigFactory.parseString(configString)
      val transformHandlerFactory = TransformHandlerFactory()
      val sut =transformHandlerFactory.getHandler(config, queryConfig)
      assert(sut == sparkHandler)
    }
  }
}
