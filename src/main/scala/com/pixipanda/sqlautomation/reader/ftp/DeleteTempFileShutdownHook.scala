package com.pixipanda.sqlautomation.reader.ftp

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

/**
  * Delete the temp file created during spark shutdown
  */
class DeleteTempFileShutdownHook(fileLocation: String) extends Thread {

  val logger: Logger = Logger.getLogger(getClass.getName)

  override def run(): Unit = {
    val dir = FileUtils.getFile(fileLocation)
    dir.listFiles().foreach(file => {
      logger.info("Deleting " + file )
      FileUtils.deleteQuietly(file)
    })
  }
}