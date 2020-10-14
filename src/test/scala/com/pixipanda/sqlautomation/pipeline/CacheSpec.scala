package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class CacheSpec extends FunSpec with TestingSparkSession{

  override protected def beforeAll(): Unit = {
    TestUtils.createEmployeeTable()
    TestUtils.populateTable("test_db1", "employee", TestUtils.empDF)
    TestUtils.createDeptTable()
    TestUtils.populateTable("test_db1", "department", TestUtils.deptDF)
    TestUtils.createEmpJoinDeptPartitionTable()
  }


  describe("Cache Spec") {

    describe("Functionality") {
      it("should join employee and department table and save to hive partitioned table  using cache_hive_to_hive_partition config file") {
        TestUtils.runPipeline("src/test/resources/jobs/cache_hive_to_hive_partition/cache_hive_to_hive_partition.conf")
        val df = spark.sql("SELECT * from test_db1.empDeptPartition where dept_name = 'Finance'")
        df.show()
        val sut = df.count()
        assert(sut == 3)
      }
    }
  }

}
