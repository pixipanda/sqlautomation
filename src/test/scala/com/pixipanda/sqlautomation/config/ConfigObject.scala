package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.extract.ExtractConfig
import com.pixipanda.sqlautomation.config.load.LoadConfig
import com.pixipanda.sqlautomation.config.transform.{SQLConfig, SQLTransformConfig, TransformConfig}
import com.pixipanda.sqlautomation.constants.ETL.TRANSFORM

object ConfigObject {

  val ftpOptions: Map[String, String] = Map(
    "server" -> "abc.com",
    "username" -> "user",
    "password" -> "ftpPassword",
    "privateKey" -> "privateKey"
  )
  val ftpSourceConfig: SourceConfig = SourceConfig("ftpServer", None, ftpOptions)

  val csvOptions: Map[String, String] = Map(
    "path" -> "/path/to/abc.csv",
    "format" -> "csv",
    "header" -> "true",
    "delimiter" -> "|"
  )
  val csvSourceConfig: SourceConfig = SourceConfig("csv", None, csvOptions)

  val extractConfig: ExtractConfig = ExtractConfig(List(ftpSourceConfig))


  val job1sqlTransformConfig1: SQLTransformConfig = SQLTransformConfig(1, None, "SELECT * FROM db.table", Some("Job1view1"), TRANSFORM)
  val job1sqlTransformConfig2: SQLTransformConfig = SQLTransformConfig(2, None, "SELECT * FROM db.table2", Some("Job1view2"), TRANSFORM)
  val job1SqlConfig: SQLConfig = SQLConfig(1, "sqlFile1.conf", List(job1sqlTransformConfig1, job1sqlTransformConfig2))
  val job1TransformConfig: TransformConfig = TransformConfig(List(job1SqlConfig))

  val job3sqlTransformConfig1: SQLTransformConfig = SQLTransformConfig(1, None, "SELECT * FROM db.table", Some("Job3view1"), TRANSFORM)
  val job3sqlTransformConfig2: SQLTransformConfig = SQLTransformConfig(2, None, "SELECT * FROM db.table2", Some("Job3view2"), TRANSFORM)
  val job3SqlConfig: SQLConfig = SQLConfig(1, "sqlFile1.conf", List(job3sqlTransformConfig1, job3sqlTransformConfig2))
  val job3TransformConfig: TransformConfig = TransformConfig(List(job3SqlConfig))

  val hiveOptions: Map[String, String] = Map(
    "db" -> "hiveDb1",
    "table" -> "hiveTable1",
    "format" -> "orc",
    "mode" -> "overwrite"
  )
  val hiveSinkConfig: SinkConfig = SinkConfig("hive", None, hiveOptions)
  val csvSinkConfig: SinkConfig = SinkConfig("csv", None,  csvOptions)
  val loadConfig: LoadConfig = LoadConfig(List(hiveSinkConfig))
}
