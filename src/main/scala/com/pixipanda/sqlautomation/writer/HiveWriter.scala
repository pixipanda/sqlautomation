package com.pixipanda.sqlautomation.writer

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.config.common.SinkConfig
import com.pixipanda.sqlautomation.container.DContainer
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col

case class HiveWriter(sinkConfig: SinkConfig) extends Writer with Spark{

  val logger: Logger = Logger.getLogger(getClass.getName)

  override def write(container: DContainer): Unit = {


    val db = sinkConfig.options("db")
    val table = sinkConfig.options("table")

    logger.info(s"Saving data to ${sinkConfig.sinkType} dbName: $db tableName: $table")

    val df = sinkConfig.repartition match {
      case Some(columns) =>
        container.dfs.head.repartition(columns.map(col): _*)
      case None =>
        container.dfs.head
    }
    df.write
      .format(sinkConfig.options("format"))
      .mode(sinkConfig.options("mode"))
      .insertInto(s"$db.$table")
  }
}