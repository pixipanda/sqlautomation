package com.pixipanda.sqlautomation.config.transform

import com.pixipanda.sqlautomation.config.{ConfigObject, Utils}
import org.scalatest.FunSpec
import com.pixipanda.sqlautomation.constants.ETL.TRANSFORM


class SQLTransformConfigSpec extends FunSpec{

  describe("Parse simple SQL Transform config") {

    it("should parse simple SQL Transform config from job1.conf") {

      val expected = ConfigObject.job1sqlTransformConfig1
      val etlConfig = Utils.buildConfig("src/test/resources/jobs/job1/job1.conf")
      val sut = etlConfig.transformConfig.get.sqlConfigs.head.sqlTransformConfigs.head
      assert(sut == expected)

    }
  }

}
