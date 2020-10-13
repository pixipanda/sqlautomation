package com.pixipanda.sqlautomation.reader.ftp

import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}

/**
  * Delete the temp file created during spark shutdown
  */
class DeleteTempFileShutdownHook(fileLocation: String) extends Thread {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  override def run(): Unit = {
    val dir = FileUtils.getFile(fileLocation)
    dir.listFiles().foreach(file => {
      LOGGER.info("Deleting " + file )
      FileUtils.deleteQuietly(file)
    })
  }
}