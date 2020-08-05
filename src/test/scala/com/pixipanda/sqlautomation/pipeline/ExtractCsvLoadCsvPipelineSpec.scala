package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class ExtractCsvLoadCsvPipelineSpec extends FunSpec {


  describe("Csv Extract Csv Load Pipeline Spec") {

    describe("Functionality") {

      it("should extract csv file and load csv file using job4 config file") {

        TestUtils.runPipeline("src/test/resources/jobs/job4/job4.conf")

      }
    }
  }

}
