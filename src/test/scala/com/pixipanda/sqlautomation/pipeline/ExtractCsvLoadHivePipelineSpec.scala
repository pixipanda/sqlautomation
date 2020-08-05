package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class ExtractCsvLoadHivePipelineSpec extends FunSpec with  TestingSparkSession{

  override protected def beforeAll(): Unit = {
    TestUtils.createEmployeeTable()
  }

  describe("Csv Extract Hive Load Pipeline Spec") {

    describe("Functionality") {

      it("should extract csv file and load to hive as orc using job5 config file") {

        TestUtils.runPipeline("src/test/resources/jobs/job5/job5.conf")

        val df = spark.sql("SELECT * from test_db1.employee")
        val sut = TestUtils.employeeDFToSeq(df)
        assert(sut.sortBy(_.emp_id) == TestUtils.employees)

      }
    }
  }
}
