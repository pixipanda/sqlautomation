package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class CsvToHiveSpec extends FunSpec with  TestingSparkSession{

  override protected def beforeAll(): Unit = {
    TestUtils.createEmployeeTable()
  }

  describe("Csv Extract Hive Load Pipeline Spec") {

    describe("Functionality") {

      it("should extract csv file and load to hive as orc using csv_to_hive config file") {
        val expected = TestUtils.employees

        TestUtils.runPipeline("src/test/resources/jobs/csv_to_hive/csv_to_hive.conf")
        val df = spark.sql("SELECT * from test_db1.employee")
        val sut = TestUtils.employeeDFToSeq(df)
        assert(sut.sortBy(_.emp_id) == expected)
      }
    }
  }
}