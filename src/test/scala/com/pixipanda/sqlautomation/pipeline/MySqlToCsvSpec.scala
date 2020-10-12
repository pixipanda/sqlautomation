package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class MySqlToCsvSpec extends FunSpec with TestingSparkSession{

  describe(" Mysql to Csv Spec") {

    describe("Functionality") {

      it("should transfer data from mysql to csv using mysql_to_csv config file") {
        TestUtils.runPipeline("src/test/resources/jobs/mysql_to_csv/mysql_to_csv.conf")
        val csvOptions = Map("header" -> "true", "inferSchema" -> "true")
        val df = spark.read.options(csvOptions).csv("/home/hduser/data/india/store/classicmodels/employees/employees.csv")
        df.show(false)
      }
    }
  }
}
