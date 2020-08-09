package com.pixipanda.sqlautomation.config.transform

import com.pixipanda.sqlautomation.config.ConfigUtils
import org.scalatest.FunSpec

class SQLConfigSpec extends FunSpec{

  describe("Simple SQL Config Parsing") {

    it("should parse multiple sql transform config from csv_transform_hive config file") {

      val expected = ConfigUtils.job1SqlConfig
      val etlConfig = ConfigUtils.buildConfig("src/test/resources/jobs/csv_transform_hive/csv_transform_hive.conf")
      val sut = etlConfig.transformConfig.get.sqlConfigs.head
      assert(sut == expected)
    }
  }
}