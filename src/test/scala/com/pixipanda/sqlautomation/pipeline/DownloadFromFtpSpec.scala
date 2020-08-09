package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.utils.TestUtils
import org.scalatest.FunSpec

class DownloadFromFtpSpec extends FunSpec {

  describe("Download from Pipeline Spec") {

    describe("Functionality") {

      it("should download csv file from FtpServer config file") {
        val expected = TestUtils.employees
        /*
         TBD
         TestUtils.runPipeline("src/test/resources/jobs/download_from_ftp/download_from_ftp.conf")
         */
      }
    }
  }

}
