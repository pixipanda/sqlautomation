package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.common.{SinkConfig, SourceConfig}
import com.pixipanda.sqlautomation.config.etl.extract.ExtractConfig
import com.pixipanda.sqlautomation.config.etl.load.LoadConfig
import com.pixipanda.sqlautomation.config.etl.transform.{QueryConfig, SQLConfig, TransformConfig}
import com.pixipanda.sqlautomation.constants.ETL.TRANSFORM

object ConfigUtils {

  val ftpSourceOptions: Map[String, String] = Map(
    "host" -> "pixipanda",
    "username" -> "hduser",
    "privateKey" -> "C:\\workspace\\Engineering\\passwordless\\private_key.ppk",
    "tempLocation" -> "C:\\tmp\\local_tmp",
    "hdfsTempLocation" -> "/tmp/hdfs_tmp",
    "path" -> "/tmp/csvfiles/input/employee.csv"
  )
  val ftpSourceConfig: SourceConfig = SourceConfig("ftpServer", None, ftpSourceOptions, None, None)

  val csvSourceOptions: Map[String, String] = Map(
    "path" -> "/home/hduser/data/india/wf/employee/employee.csv",
    "format" -> "csv",
    "header" -> "true",
    "inferSchema" -> "true"
  )
  val csvSourceConfig: SourceConfig = SourceConfig("csv", None, csvSourceOptions, None, None)
  val csvSourceSchemaConfig: SourceConfig = SourceConfig("csv", None, csvSourceOptions, None, Some("src/test/resources/jobs/csv_schema_to_hive/schema.avsc"))
  val csvSourceViewConfig: SourceConfig = SourceConfig("csv", Some("employeeView"), csvSourceOptions, None, None)

  val hiveSourceOptions: Map[String, String] = Map(
    "db" -> "test_db1",
    "table" -> "employee",
    "format" -> "orc"
  )
  val hiveSourceConfig: SourceConfig = SourceConfig("hive", None, hiveSourceOptions, None, None)


  val extractConfig: ExtractConfig = ExtractConfig(List(ftpSourceConfig))


  val job1QueryConfig1: QueryConfig = QueryConfig(1, None, "SELECT * FROM employeeView where emp_dept_id = 10", None, None, TRANSFORM)
  val job1QueryConfig2: QueryConfig = QueryConfig(2, None, "SELECT * FROM db.table2", Some("Job1view2"), None, TRANSFORM)
  val job1SqlConfig: SQLConfig = SQLConfig(1, "sqlFile1.conf", List(job1QueryConfig1))
  val job1TransformConfig: TransformConfig = TransformConfig(List(job1SqlConfig))

  val job3QueryConfig1: QueryConfig = QueryConfig(1, None, "SELECT * FROM test_db1.employee", Some("employeeView"), None, TRANSFORM)
  val job3QueryConfig2: QueryConfig = QueryConfig(2, None, "SELECT * FROM employeeView where emp_dept_id = 10", Some("Job3view2"), None, TRANSFORM)
  val job3SqlConfig: SQLConfig = SQLConfig(1, "sqlFile1.conf", List(job3QueryConfig1, job3QueryConfig2))
  val job3TransformConfig: TransformConfig = TransformConfig(List(job3SqlConfig))


  val ftpSinkOptions: Map[String, String] = Map(
    "server" -> "lxhdpsatqa001",
    "username" -> "hdpbatch"
  )


  val csvSinkOptions: Map[String, String] = Map(
    "path" -> "/tmp/csvfiles/output",
    "format" -> "csv",
    "header" -> "true",
    "mode" -> "overwrite"
  )

  val csvSinkConfig: SinkConfig = SinkConfig("csv", None,  csvSinkOptions)

  val hiveSinkOptions: Map[String, String] = Map(
    "db" -> "test_db1",
    "table" -> "employee",
    "format" -> "orc",
    "mode" -> "overwrite"
  )
  val hiveSinkConfig: SinkConfig = SinkConfig("hive", None, hiveSinkOptions)

  val loadConfig: LoadConfig = LoadConfig(List(hiveSinkConfig))


  def buildConfig(configFile: String): AppConfig = {
    ConfigRegistry.parseConfig(configFile)
    AppConfig.parse(ConfigRegistry.getConfig)
  }
}
