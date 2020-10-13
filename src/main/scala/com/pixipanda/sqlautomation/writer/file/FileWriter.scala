package com.pixipanda.sqlautomation.writer.file

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.config.common.SinkConfig
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.utils.FileUtils
import com.pixipanda.sqlautomation.writer.Writer
import org.slf4j.{Logger, LoggerFactory}

case class FileWriter(sinkConfig: SinkConfig) extends Writer with Spark{

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  override def write(container: DContainer): Unit = {

    val saveOptions = sinkConfig.options

    LOGGER.info(s"Saving as ${sinkConfig.sinkType} file. save options: $saveOptions")

    val df = container.dfs.head

    df.coalesce(1)
      .write
      .options(saveOptions)
      .format(saveOptions("format"))
      .mode(saveOptions("mode"))
      .save(saveOptions("path"))

    saveOptions.get("path").foreach(path => {
      saveOptions.get("fileName").foreach(fileName => {
        LOGGER.info(s"Renaming file as $fileName")
        FileUtils.renamePartFile(sc.hadoopConfiguration, path, fileName)
      })
    })
  }
}