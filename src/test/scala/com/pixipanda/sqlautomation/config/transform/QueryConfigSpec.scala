package com.pixipanda.sqlautomation.config.transform

import com.pixipanda.sqlautomation.config.ConfigUtils
import org.scalatest.FunSpec

class QueryConfigSpec extends FunSpec{

  describe("Parse simple SQL Transform config") {

    it("should parse simple SQL Transform config from csv_transform_hive.conf") {

      val expected = ConfigUtils.job1QueryConfig1
      val etlConfig = ConfigUtils.buildConfig("src/test/resources/jobs/csv_transform_hive/csv_transform_hive.conf")
      val sut = etlConfig.transformConfig.get.sqlConfigs.head.queryConfigs.head
      assert(sut == expected)

    }
  }

}