package com.pixipanda.sqlautomation.config.transform


import com.pixipanda.sqlautomation.config.ConfigUtils
import org.scalatest.FunSpec

class TransformConfigSpec extends FunSpec {

  describe("Simple Transform Config Parsing") {

    it("should parse simple transform config from csv_transform_hive.conf") {

      val expected = Some(ConfigUtils.job1TransformConfig)
      val etlConfig = ConfigUtils.buildConfig("src/test/resources/jobs/csv_transform_hive/csv_transform_hive.conf")
      val sut = etlConfig.transformConfig
      assert(sut == expected)
    }
  }

}