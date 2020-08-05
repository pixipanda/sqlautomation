package com.pixipanda.sqlautomation.reader.file

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.config.SourceConfig
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.reader.Reader
import org.apache.log4j.Logger

/*
 FileReader class is uused to read data from different files sources like CSV, XML, JSON, ORC, PARQUET, FTPSERVER etc

*/
case class FileReader(sourceConfig: SourceConfig) extends Reader with Spark{

  val logger: Logger = Logger.getLogger(getClass.getName)

  /*
    This function will read data from different file sources like
    CSV, XML, JSON, ORC, PARQUET, FTPSERVER etc.
    Returns: a container that has view and the dataFrame of the data read from the sources
  */
  def read:DContainer = {

    val fileOptions = sourceConfig.options

    logger.info(s"Reading from ${sourceConfig.sourceType} file. Source option is $fileOptions")

    val df = spark.read
      .format(fileOptions("format"))
      .options(fileOptions)
      .load(fileOptions("path"))

    DContainer(sourceConfig.viewName, df)
  }
}
