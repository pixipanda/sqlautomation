package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.config.ConfigObject
import com.pixipanda.sqlautomation.config.extract.ExtractConfig
import com.pixipanda.sqlautomation.config.load.LoadConfig
import com.pixipanda.sqlautomation.config.transform.SQLConfig
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.handler.Handler
import com.pixipanda.sqlautomation.handler.extract.ExtractHandler
import com.pixipanda.sqlautomation.handler.load.LoadHandler
import com.pixipanda.sqlautomation.handler.transform.SQLTransformHandler
import com.pixipanda.sqlautomation.reader.file.FileReader
import com.pixipanda.sqlautomation.writer.HiveWriter
import com.pixipanda.sqlautomation.writer.file.FileWriter
import org.scalatest.FunSpec

import scala.collection.mutable

class HandlerFactorySpec extends  FunSpec{

  describe("Handler Factory Spec") {

    describe("Get Extract Handler") {

      val csvSourceConfig = ConfigObject.csvSourceConfig
      val ftpSourceConfig = ConfigObject.ftpSourceConfig
      val csvReader = FileReader(csvSourceConfig)
      val ftpReader = FileReader(ftpSourceConfig)

      it("should create extract handler with csv Reader") {


        val extractConfig = ExtractConfig(List(csvSourceConfig))
        val expected = ExtractHandler(List(csvReader))
        val sut = HandlerFactory.getHandler(extractConfig)
        assert(sut  == expected)
      }

      it("should create extract handler with multiple reader") {

        val extractConfig = ExtractConfig(List(csvSourceConfig, ftpSourceConfig))
        val expected = ExtractHandler(List(csvReader, ftpReader))
        val sut = HandlerFactory.getHandler(extractConfig)
        assert(sut == expected)
      }
    }

    describe("Get TransformHandler Handler") {

      val sqlTransformConfig1 = ConfigObject.job1sqlTransformConfig1
      val sqlTransformConfig2 = ConfigObject.job1sqlTransformConfig2
      val sqlConfig = SQLConfig(1, "sqlFile1.conf", List(sqlTransformConfig1, sqlTransformConfig2))

      it("should create sql transform handler") {

        val query = sqlTransformConfig1.query
        val viewName = sqlTransformConfig1.viewName
        val expected = SQLTransformHandler(query, viewName)
        val sut = HandlerFactory.getHandler(sqlTransformConfig1)
        assert(sut == expected)
      }

      it("should create sql transform handler with multiple transformers ") {

        val sqlTransformHandler1 = SQLTransformHandler(sqlTransformConfig1.query, sqlTransformConfig1.viewName)
        val sqlTransformHandler2 = SQLTransformHandler(sqlTransformConfig2.query, sqlTransformConfig2.viewName)
        val expected = List(sqlTransformHandler1, sqlTransformHandler2)
        val handlers = mutable.ListBuffer[Handler[DContainer, DContainer]]()
        sqlConfig.sqlTransformConfigs.foreach(sqlTransformConfig => {
          val handler = HandlerFactory.getHandler(sqlTransformConfig)
          handlers.append(handler)
        })
        assert(handlers.toList == expected)
      }
    }


    describe("Get Load Handler") {

      val csvSinkConfig = ConfigObject.csvSinkConfig
      val hiveSinkConfig = ConfigObject.hiveSinkConfig
      val csvWriter = FileWriter(csvSinkConfig)
      val hiveWriter = HiveWriter(hiveSinkConfig)

      it("should create load handler with csv Writer") {


        val loadConfig = LoadConfig(List(csvSinkConfig))
        val csvWriter = FileWriter(csvSinkConfig)
        val expected = LoadHandler(List(csvWriter))
        val sut = HandlerFactory.getHandler(loadConfig)
        assert(sut  == expected)
      }

      it("should create load handler with multiple writers") {

        val loadConfig = LoadConfig(List(csvSinkConfig, hiveSinkConfig))
        val expected = LoadHandler(List(csvWriter, hiveWriter))
        val sut = HandlerFactory.getHandler(loadConfig)
        assert(sut == expected)
      }
    }
  }
}
