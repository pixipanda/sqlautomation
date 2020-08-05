package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.config.SourceConfig
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class ExtractCsvLoadMultiplePipelineSpec extends FunSpec with  TestingSparkSession{

  override protected def beforeAll(): Unit = {
    TestUtils.createEmployeeTable()
  }

  describe("Extract Csv Load Multiple Pipeline Spec") {

    describe("Functionality") {

      val csvOptions: Map[String, String] = Map(
        "path" -> "/tmp/csvfiles/write/",
        "format" -> "csv",
        "header" -> "true",
        "inferSchema" -> "true"
      )
      val csvSourceConfig: SourceConfig = SourceConfig("csv", None, csvOptions)

      it("should extract csv file and load to csv file and hive as orc using job6 config file") {

        TestUtils.runPipeline("src/test/resources/jobs/job6/job6.conf")

        val df = spark.sql("SELECT * from test_db1.employee")
        val hiveSut = TestUtils.employeeDFToSeq(df)
        assert(hiveSut.sortBy(_.emp_id) == TestUtils.employees)

        val csvSut = TestUtils.readEmployee(csvSourceConfig)
        assert(csvSut.sortBy(_.emp_id) == TestUtils.employees)
      }
    }
  }
}
