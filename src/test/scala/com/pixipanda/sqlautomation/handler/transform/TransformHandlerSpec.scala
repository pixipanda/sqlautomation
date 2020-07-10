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
        |   q1 = "SELECT * FROM db.table"
        | }
      """.stripMargin


    val queryConfig = TransformConfig(1, "SELECT * FROM db.table", "q1view", ETL.TRANSFORM)
    val sparkHandler = SparkTransformHandler("SELECT * FROM db.table", "q1view")

    it("should create Spark Load Handler") {
      ConfigFactory.parseString(configString)
      val transformHandlerFactory = TransformHandlerFactory()
      val sut =transformHandlerFactory.getHandler(queryConfig)
      assert(sut == sparkHandler)
    }
  }
}
