package com.pixipanda.sqlautomation.config.transform

import com.pixipanda.sqlautomation.config.{ConfigObject, Utils}
import org.scalatest.FunSpec

class TransformConfigSpec extends FunSpec {

  describe("Simple Transform Config Parsing") {

    it("should parse simple transform config from job1.conf") {

      val expected = Some(ConfigObject.job1TransformConfig)
      val etlConfig = Utils.buildConfig("src/test/resources/jobs/job1/job1.conf")
      val sut = etlConfig.transformConfig
      assert(sut == expected)
    }
  }

}
