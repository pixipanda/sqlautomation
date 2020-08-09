package com.pixipanda.sqlautomation.reader.file

import com.pixipanda.sqlautomation.config.common.SourceConfig
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class CsvReaderSpec extends FunSpec{

  describe("Csv File Reader Spec") {

    describe("Functionality") {

      val csvOptions = Map(
        "path" -> "/tmp/csvfiles/input/employee.csv",
        "format" -> "csv",
        "header" -> "true",
        "inferSchema" -> "true"
      )
      val csvSourceConfig = SourceConfig("csv", None, csvOptions)

      it("should read csv file1") {

        val expected = TestUtils.employees
        val csvReader = FileReader(csvSourceConfig)
        val container = csvReader.read

        val sut = TestUtils.employeeDFToSeq(container.dfs.head)
        assert(sut == expected)
      }
    }
  }

}