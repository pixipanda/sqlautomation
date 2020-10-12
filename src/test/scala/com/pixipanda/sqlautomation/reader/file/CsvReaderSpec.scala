package com.pixipanda.sqlautomation.reader.file

import com.pixipanda.sqlautomation.config.common.SourceConfig
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class CsvReaderSpec extends FunSpec{

  describe("Csv File Reader Spec") {

    describe("Functionality") {

      val csvOptions = Map(
        "path" -> "/home/hduser/data/india/wf/employee/employee.csv",
        "format" -> "csv",
        "header" -> "true"
      )
      val csvSourceConfig = SourceConfig("csv", None, csvOptions + ("inferSchema" -> "true"), None)
      val csvSourceSchemaConfig = SourceConfig("csv", None, csvOptions, Some("src/test/resources/jobs/csv_schema_to_hive/schema.avsc"))

      it("should read csv file from /home/hduser/data/india/wf/employee/employee.csv and create a dataFrame") {

        val expected = TestUtils.employees
        val csvReader = FileReader(csvSourceConfig)
        val container = csvReader.read

        val sut = TestUtils.employeeDFToSeq(container.dfs.head)
        assert(sut == expected)
      }


      it("should read csv file from /home/hduser/data/india/wf/employee/employee.csv with given given schema and create a dataFrame") {

        val expected = TestUtils.employees
        val csvReader = FileReader(csvSourceSchemaConfig)
        val container = csvReader.read

        val sut = TestUtils.employeeDFToSeq(container.dfs.head)
        assert(sut == expected)
      }
    }
  }

}