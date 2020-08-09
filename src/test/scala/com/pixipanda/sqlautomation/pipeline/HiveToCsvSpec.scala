package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class HiveToCsvSpec extends FunSpec with TestingSparkSession{

  override protected def beforeAll(): Unit = {
    TestUtils.createEmployeeTable()
    TestUtils.populateTable("test_db1","employee", TestUtils.empDF)
  }


  describe(" Hive to Csv Spec") {

    describe("Functionality") {

      it("should transfer data from hive to csv using hive_to_csv config file") {
        val expected = TestUtils.employees.filter(_.emp_dept_id == 10)
        TestUtils.runPipeline("src/test/resources/jobs/hive_to_csv/hive_to_csv.conf")
        val csvOptions = Map("header" -> "true", "inferSchema" -> "true")
        val df = spark.read.options(csvOptions).csv("/tmp/csvfiles/output")
        val sut = TestUtils.employeeDFToSeq(df)
        assert(sut.sortBy(_.emp_id) == expected)
      }
    }
  }
}
