package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class CsvTransformHiveSpec extends FunSpec with  TestingSparkSession{

  override protected def beforeAll(): Unit = {
    TestUtils.createEmployeeTable()
  }

  describe("Extract from csv ,transform and load to hive Pipeline Spec") {

    describe("Functionality") {

      it("should extract csv file, do simple transformation and load to hive as orc using csv_transform_hive config file") {
        val expected = TestUtils.employees.filter(_.emp_dept_id == 10)
        TestUtils.runPipeline("src/test/resources/jobs/csv_transform_hive/csv_transform_hive.conf")
        val df = spark.sql("SELECT * from test_db1.employee")
        val sut = TestUtils.employeeDFToSeq(df)
        assert(sut.sortBy(_.emp_id) == expected)
      }
    }
  }
}