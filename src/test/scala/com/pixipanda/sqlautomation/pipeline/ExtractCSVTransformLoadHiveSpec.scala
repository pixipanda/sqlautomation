package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class ExtractCSVTransformLoadHiveSpec extends FunSpec with  TestingSparkSession{

  override protected def beforeAll(): Unit = {
    TestUtils.createEmployeeTable()
  }

  describe("Csv Extract Hive Load Pipeline Spec") {

    describe("Functionality") {

      it("should extract csv file, do simple transformation and load to hive as orc using job7 config file") {

        TestUtils.runPipeline("src/test/resources/jobs/job7/job7.conf")

        val expected = TestUtils.employees.filter(_.emp_dept_id == 10)

        val df = spark.sql("SELECT * from test_db1.employee")
        df.show()
        val sut = TestUtils.employeeDFToSeq(df)
        assert(sut.sortBy(_.emp_id) == expected)

      }
    }
  }
}