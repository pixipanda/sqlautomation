package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.{ConfigRegistry, ConfigUtils}
import com.pixipanda.sqlautomation.handler.extract.ExtractHandler
import com.pixipanda.sqlautomation.handler.load.LoadHandler
import com.pixipanda.sqlautomation.handler.transform.QueryTransformHandler
import com.pixipanda.sqlautomation.reader.file.FileReader
import com.pixipanda.sqlautomation.writer.HiveWriter
import com.pixipanda.sqlautomation.writer.file.FileWriter
import org.scalatest.FunSpec

class HandlerFactorySpec extends  FunSpec{

  describe("Handler Factory Spec") {

    describe("Get Extract Handler") {

      val ftpReader = FileReader(ConfigUtils.ftpSourceConfig)

      it("should create extract handler with csv Reader from csv_transform_hive.conf") {
        val csvReader = FileReader(ConfigUtils.csvSourceViewConfig)
        val expected = ExtractHandler(List(csvReader))

        ConfigRegistry.parseConfig("src/test/resources/jobs/csv_transform_hive/csv_transform_hive.conf")
        val extractConfig = ConfigRegistry.appConfig.extractConfig.get
        val sut = HandlerFactory.getHandler(extractConfig)
        assert(sut  == expected)
      }
    }

    describe("Get TransformHandler Handlers") {

      it("should create query transform handler from csv_transform_hive.conf file") {
        val expected = List(QueryTransformHandler("SELECT * FROM employeeView where emp_dept_id = 10", None))
        ConfigRegistry.parseConfig("src/test/resources/jobs/csv_transform_hive/csv_transform_hive.conf")
        val transformConfig = ConfigRegistry.appConfig.transformConfig.get
        val sut = HandlerFactory.getHandler(transformConfig)
        assert(sut == expected)
      }

      it("should create sql transform handler with multiple transformers from hive_to_hive.conf") {
        val queryTransformHandler1 = QueryTransformHandler("SELECT * FROM test_db1.employee", Some("employeeView"))
        val queryTransformHandler2 = QueryTransformHandler("SELECT * FROM test_db1.department", Some("departmentView"))
        val queryTransformHandler3 = QueryTransformHandler("SELECT emp_id, name, dept_name FROM employeeView e JOIN departmentView d ON e.emp_dept_id == d.dept_id",None)
        val expected = List(queryTransformHandler1, queryTransformHandler2, queryTransformHandler3)
        ConfigRegistry.parseConfig("src/test/resources/jobs/hive_to_hive/hive_to_hive.conf")
        val transformConfig = ConfigRegistry.appConfig.transformConfig.get
        val sut = HandlerFactory.getHandler(transformConfig)
        assert(sut == expected)
      }
    }


    describe("Get Load Handler") {

      val csvSinkConfig = ConfigUtils.csvSinkConfig
      val hiveSinkConfig = ConfigUtils.hiveSinkConfig
      val csvWriter = FileWriter(csvSinkConfig)
      val hiveWriter = HiveWriter(hiveSinkConfig)

      it("should create load handler with hive Writer from csv_transform_hive.conf") {
        val expected = LoadHandler(List(hiveWriter))
        ConfigRegistry.parseConfig("src/test/resources/jobs/csv_transform_hive/csv_transform_hive.conf")
        val loadConfig = ConfigRegistry.appConfig.loadConfig.get
        val sut = HandlerFactory.getHandler(loadConfig)
        assert(sut  == expected)
      }
    }
  }
}
