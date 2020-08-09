package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.utils.TestUtils
import com.pixipanda.sqlautomation.utils.TestUtils.EmpDept
import org.scalatest.FunSpec

class HiveToHiveSpec extends FunSpec with TestingSparkSession{

  override protected def beforeAll(): Unit = {
    TestUtils.createEmployeeTable()
    TestUtils.populateTable("test_db1", "employee", TestUtils.empDF)
    TestUtils.createDeptTable()
    TestUtils.populateTable("test_db1", "department", TestUtils.deptDF)
    TestUtils.createEmpJoinDeptTable()
  }


  describe(" Hive to Hive Spec") {

    describe("Functionality") {

      it("should join employee and department table and save to hive  using hive_to_hive config file") {
        val expected = TestUtils.empDept
        TestUtils.runPipeline("src/test/resources/jobs/hive_to_hive/hive_to_hive.conf")
        val df = spark.sql("SELECT * from test_db1.empDept")
        val sut = df.collect
          .toList
          .map(row => {
            val emp_id = row.getAs[Int]("emp_id")
            val name = row.getAs[String]("name")
            val dept_name = row.getAs[String]("dept_name")
            EmpDept(emp_id, name, dept_name)
          })

        assert(sut.sortBy(_.emp_id) == expected)
      }
    }
  }
}
