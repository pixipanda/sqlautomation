package com.pixipanda.sqlautomation.config.transform

import com.pixipanda.sqlautomation.config.{ConfigObject, Utils}
import com.pixipanda.sqlautomation.constants.ETL.TRANSFORM
import org.scalatest.FunSpec

class SQLConfigSpec extends FunSpec{

  describe("Simple SQL Config Parsing") {


    it("should parse multiple sql transform config from job1 config file") {

      val expected = ConfigObject.job1SqlConfig
      val etlConfig = Utils.buildConfig("src/test/resources/jobs/job1/job1.conf")
      val sut = etlConfig.transformConfig.get.sqlConfigs.head
      assert(sut == expected)
    }
  }
}
