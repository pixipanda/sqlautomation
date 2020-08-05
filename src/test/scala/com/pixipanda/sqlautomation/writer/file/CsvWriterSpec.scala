package com.pixipanda.sqlautomation.writer.file

import com.pixipanda.sqlautomation.config.{SinkConfig, SourceConfig}
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.reader.file.FileReader
import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class CsvWriterSpec extends FunSpec{
  describe("Csv File Writer Spec") {

    describe("Functionality") {

      val csvOptions = Map(
        "path" -> "/tmp/csvfiles/write/",
        "format" -> "csv",
        "header" -> "true",
        "mode" -> "overwrite"
      )

      val csvSinkConfig = SinkConfig("csv", None, csvOptions)
      val csvSourceConfig = SourceConfig("csv", None, csvOptions ++ Map("inferSchema" -> "true"))

      it("should write csv file") {

        val empDF = TestUtils.empDF
        val writeContainer = DContainer(empDF)
        val csvWriter = FileWriter(csvSinkConfig)
        csvWriter.write(writeContainer)

        val sut = TestUtils.readEmployee(csvSourceConfig)
        assert(sut.sortBy(_.emp_id) == TestUtils.employees)
      }
    }
  }
}
