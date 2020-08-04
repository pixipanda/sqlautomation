package com.pixipanda.sqlautomation.config

import com.pixipanda.sqlautomation.config.extract.ExtractConfig
import com.pixipanda.sqlautomation.config.load.LoadConfig
import com.pixipanda.sqlautomation.config.transform.{SQLConfig, SQLTransformConfig, TransformConfig}
import com.pixipanda.sqlautomation.constants.ETL.TRANSFORM

object Utils {

  val ftpOptions: Map[String, String] = Map(
    "server" -> "abc.com",
    "username" -> "user",
    "password" -> "ftpPassword",
    "privateKey" -> "privateKey"
  )
  val ftpSourceConfig: SourceConfig = SourceConfig("ftpServer", None, ftpOptions)
  val job1ExtractConfig: ExtractConfig = ExtractConfig(List(ftpSourceConfig))


  val Job1SqlTransformConfig1: SQLTransformConfig = SQLTransformConfig(1, None, "SELECT * FROM db.table", Some("Job1view1"), TRANSFORM)
  val Job1SqlTransformConfig2: SQLTransformConfig = SQLTransformConfig(2, None, "SELECT * FROM db.table2", Some("Job1view2"), TRANSFORM)
  val Job1sqlConfig: SQLConfig = SQLConfig(1, "sqlFile1.conf", List(Job1SqlTransformConfig1, Job1SqlTransformConfig1))
  val transformConfig: TransformConfig = TransformConfig(List(Job1sqlConfig))

  val hiveOptions: Map[String, String] = Map(
    "db" -> "hiveDb1",
    "table" -> "hiveTable1",
    "format" -> "orc",
    "mode" -> "overwrite"
  )
  val hiveSinkConfig: SinkConfig = SinkConfig("hive", None, hiveOptions)
  val job1LoadConfig: LoadConfig = LoadConfig(List(hiveSinkConfig))


  def buildConfig(configFile: String): AppConfig = {
    ConfigRegistry.setEnv("qa")
    ConfigRegistry.parseConfig(configFile)
    AppConfig.parse(ConfigRegistry.getConfig)
  }


}
