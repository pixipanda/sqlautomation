package com.pixipanda.sqlautomation.writer.file

import com.pixipanda.sqlautomation.config.ConfigUtils
import com.pixipanda.sqlautomation.config.common.{SinkConfig, SourceConfig}
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class CsvWriterSpec extends FunSpec{
  describe("Csv File Writer Spec") {

    describe("Functionality") {

      val csvSourceOption = Map(
        "path" -> "/tmp/csvfiles/output",
        "format" -> "csv",
        "header" -> "true",
        "inferSchema" -> "true"
      )
      val csvSourceConfig = SourceConfig("csv", None, csvSourceOption, None)


      it("should write csv file") {
        val empDF = TestUtils.empDF
        val input = new DContainer(empDF)
        val csvWriter = FileWriter(ConfigUtils.csvSinkConfig)
        csvWriter.write(input)

        val sut = TestUtils.readEmployee(csvSourceConfig)
        assert(sut.sortBy(_.emp_id) == TestUtils.employees)
      }

      it("should write csv file with given filename") {
        val csvSinkOptions: Map[String, String] = Map(
          "path" -> "/tmp/csvfiles/output",
          "format" -> "csv",
          "header" -> "true",
          "mode" -> "overwrite",
          "fileName" -> "employee.csv"
        )
        val csvSinkConfig = SinkConfig("csv", None, csvSinkOptions)

        val empDF = TestUtils.empDF
        val input = new DContainer(empDF)
        val csvWriter = FileWriter(csvSinkConfig)
        csvWriter.write(input)

        val sut = TestUtils.readEmployee(csvSourceConfig)
        assert(sut.sortBy(_.emp_id) == TestUtils.employees)
      }
    }
  }
}