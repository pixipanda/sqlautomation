package com.pixipanda.sqlautomation.reader.jdbc

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.config.common.SourceConfig
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.reader.Reader
import org.apache.log4j.Logger

case class JDBCReader(sourceConfig: SourceConfig) extends Reader with  Spark{

  val LOGGER: Logger = Logger.getLogger(getClass.getName)

  override def read: DContainer = {

    val jdbcOptions = sourceConfig.options

    LOGGER.info(s"Reading from ${sourceConfig.sourceType} file. Source option is $jdbcOptions")

    val df = spark.read
        .format(jdbcOptions("format"))
        .options(jdbcOptions)
        .load()
    DContainer(sourceConfig.viewName, df)
  }
}
