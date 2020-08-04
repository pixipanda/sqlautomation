package com.pixipanda.sqlautomation.writer.file

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.config.SinkConfig
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.writer.Writer
import org.apache.log4j.Logger

case class FileWriter(sinkConfig: SinkConfig) extends Writer with Spark{

  val logger: Logger = Logger.getLogger(getClass.getName)

  override def write(container: DContainer): Unit = {

    val saveOptions = sinkConfig.options

    logger.info(s"Saving as ${sinkConfig.sinkType} file. save options: $saveOptions")

    val df = container.dfs.head

    df.coalesce(1)
      .write
      .options(saveOptions)
      .format(saveOptions("format"))
      .mode(saveOptions("mode"))
      .save(saveOptions("path"))

  }
}
