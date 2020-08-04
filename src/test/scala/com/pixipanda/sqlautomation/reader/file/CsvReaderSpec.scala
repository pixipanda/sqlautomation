package com.pixipanda.sqlautomation.reader.file

import com.pixipanda.sqlautomation.config.SourceConfig
import com.pixipanda.sqlautomation.utils.DataObjects
import org.scalatest.FunSpec

class CsvReaderSpec extends FunSpec{

  describe("Csv File Reader Spec") {

    describe("Functionality") {

      val csvOptions = Map(
        "path" -> "/tmp/csvfiles/file1.csv",
        "format" -> "csv",
        "header" -> "true",
        "inferSchema" -> "true"
      )
      val csvSourceConfig = SourceConfig("csv", None, csvOptions)

      it("should read csv file1") {

        val expected = DataObjects.employee
        val csvReader = FileReader(csvSourceConfig)
        val container = csvReader.read
        val sut = container.dfs.head
          .collect
          .toList
          .map(row => {
            (
              row.getAs[Int]("emp_id"),
              row.getAs[String]("name"),
              row.getAs[String]("gender"),
              row.getAs[Int]("dept_id")
            )
          })
         assert(sut.sortBy(_._1) == expected)
      }
    }
  }

}
