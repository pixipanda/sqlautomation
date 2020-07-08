package com.pixipanda.sqlautomation.pipeline

import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.sqlautomation.config.{ConfigRegistry, SQLAutomate}
import com.pixipanda.sqlautomation.handler.load.HiveLoadHandler
import com.pixipanda.sqlautomation.handler.transform.SparkTransformHandler
import com.pixipanda.sqlautomation.utils.{HDFSCluster, HDFSHelper}
import org.scalatest.{BeforeAndAfterAll, FunSpec}


class PipelineSpec extends FunSpec with HDFSCluster with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    startHDFS()
    val url = getNameNodeURI
    val confFile = getNameNodeURI + "/abc.conf"
    val hdfsHelper = new HDFSHelper[String](url)
    val data =
      """
        | {
        |   q1 = "SELECT * from db.table"
        | }
      """.stripMargin
    hdfsHelper.write(confFile, data)

    val sqlFile1 =
      """
        | {
        |   q11 = "SELECT * from db.table"
        |   q12 = "SELECT * from view11"
        | }
      """.stripMargin
    hdfsHelper.write(getNameNodeURI + "/sqlFile1.conf", sqlFile1)

    val sqlFile2 =
      """
        | {
        |   q21 = "SELECT * from view11 join view12"
        |   q22 = "SELECT * from view11 join view12 join view21"
        | }
      """.stripMargin
    hdfsHelper.write(getNameNodeURI + "/sqlFile2.conf", sqlFile2)

  }


  override protected def afterAll(): Unit = {
    shutdownHDFS()
  }


  describe("Build Pipeline Spec") {

    val sparkTransformHandler11 = SparkTransformHandler("SELECT * from db.table", "view11")
    val sparkTransformHandler12 = SparkTransformHandler("SELECT * from view11", "view12")
    val sparkTransformHandler21 = SparkTransformHandler("SELECT * from view11 join view12", "view21")

    val hiveOrcOptions = Map("db" -> "db1", "table" -> "table1", "format" -> "orc", "mode" -> "overwrite")
    val hiveSaveConfig = SaveConfig("hive", Some(List("colA", "colB")), Some(true), "path1", hiveOrcOptions)
    val hiveLoadHandler = HiveLoadHandler("SELECT * from view11 join view12 join view21", hiveSaveConfig)


    describe("Pipeline contains hive load handler") {

      val hiveOrcOptions = Map("db" -> "db1", "table" -> "table1", "format" -> "orc", "mode" -> "overwrite")
      val hiveSaveConfig = SaveConfig("hive", Some(List("colA", "colB")), Some(true), "path1", hiveOrcOptions)
      val hiveLoadHandler = HiveLoadHandler("SELECT * from db.table", hiveSaveConfig)

      it("should create Hive Load Handler with orc format") {

        ConfigRegistry.setEnv("qa")
        val sqlAutomate = SQLAutomate.parseSQLAutomate(ConfigRegistry.config("src/test/resources/only_hive_load.conf"))
        val fs = getFileSystem
        val etlPipeline = ETLPipeline.buildPipeline(sqlAutomate, fs)
        val sut = etlPipeline.getHandlers
        assert(sut.size == 1)
        assert(sut.head == hiveLoadHandler)
      }
    }

    describe("Pipeline contains multiple handlers") {

      it("should create multiple handlers") {
        ConfigRegistry.setEnv("qa")
        val sqlAutomate = SQLAutomate.parseSQLAutomate(ConfigRegistry.config("src/test/resources/hive_load.conf"))
        val fs = getFileSystem
        val sparkPipeline = ETLPipeline.buildPipeline(sqlAutomate, fs)
        val sut = sparkPipeline.getHandlers
        assert(sut.size == 4)
        assert(sut== List(sparkTransformHandler11, sparkTransformHandler12, sparkTransformHandler21, hiveLoadHandler))
      }
    }
  }
}
