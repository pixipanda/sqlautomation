package com.pixipanda.sqlautomation.reader.file

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.config.SourceConfig
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.reader.Reader


case class FileReader(sourceConfig: SourceConfig) extends Reader with Spark{

  def read:DContainer = {

    val fileOptions = sourceConfig.options

    val df = spark.read
      .format(fileOptions("format"))
      .options(fileOptions)
      .load(fileOptions("path"))

    DContainer(sourceConfig.viewName, df)
  }
}
