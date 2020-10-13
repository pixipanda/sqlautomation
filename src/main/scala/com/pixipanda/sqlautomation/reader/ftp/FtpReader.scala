package com.pixipanda.sqlautomation.reader.ftp

import java.io.File

import com.pixipanda.sftpclient.SFTPClient
import com.pixipanda.sqlautomation.config.common.SourceConfig
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.reader.Reader
import com.pixipanda.sqlautomation.utils.FileUtils
import org.slf4j.{Logger, LoggerFactory}

case class FtpReader(sourceConfig: SourceConfig) extends Reader {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)


  private def getValue(param: Option[String]): String = {
    param match {
      case Some(value) => value
      case None => null
    }
  }

  private def getSFTPClient(
                             username: String,
                             password: Option[String],
                             pemFileLocation: String,
                             pemPassphrase: Option[String],
                             host: String,
                             port: Option[String],
                             cryptoKey : String,
                             cryptoAlgorithm : String) : SFTPClient = {

    val sftpPort = if (port != null && port.isDefined) {
      port.get.toInt
    } else {
      22
    }

    val cryptoEnabled = cryptoKey != null

    if (cryptoEnabled) {
      new SFTPClient(pemFileLocation, getValue(pemPassphrase), username,
        getValue(password),
        host, sftpPort, cryptoEnabled, cryptoKey, cryptoAlgorithm)
    } else {
      new SFTPClient(pemFileLocation, getValue(pemPassphrase), username,
        getValue(password), host, sftpPort)
    }
  }

  private def addShutdownHook(tempLocation: String) {
    LOGGER.info("Adding hook for file " + tempLocation)
    val hook = new DeleteTempFileShutdownHook(tempLocation)
    Runtime.getRuntime.addShutdownHook(hook)
  }

  private def copy(sftpClient: SFTPClient, source: String,
                   tempFolder: String, latest: Boolean): String = {
    var copiedFilePath: String = null
    try {
      if (latest) {
        LOGGER.info("Downloading latest from " + source + " to " + tempFolder)
        copiedFilePath = sftpClient.copyLatest(source, tempFolder)
      } else {
        LOGGER.info("Downloading " + source + " to " + tempFolder)
        copiedFilePath = sftpClient.copy(source, tempFolder)
      }
      tempFolder
    } finally {
      addShutdownHook(tempFolder)
    }
  }

  override def read: DContainer = {

    val ftpOptions = sourceConfig.options
    val username = ftpOptions.getOrElse("username", sys.error("username must be specified"))
    val password = ftpOptions.get("password")
    val defaultPrivateKey = System.getProperty("user.home") + File.separator + ".ssh/id_rsa"
    val pemFileLocation = ftpOptions.getOrElse("privateKey", defaultPrivateKey)
    val pemPassphrase = ftpOptions.get("keyPassphrase")
    val host = ftpOptions.getOrElse("host", sys.error("SFTP Host has to be provided using 'host' option"))
    val port = ftpOptions.get("port")
    val path = ftpOptions.getOrElse("path", sys.error("'path' must be specified"))
    val copyLatest = ftpOptions.getOrElse("copyLatest", "false")
    val tmpFolder = ftpOptions.getOrElse("tempLocation", System.getProperty("java.io.tmpdir"))
    val HDFSTemp = ftpOptions.getOrElse("hdfsTempLocation", tmpFolder)
    val cryptoKey = ftpOptions.getOrElse("cryptoKey", null)
    val cryptoAlgorithm = ftpOptions.getOrElse("cryptoAlgorithm", "AES")

    LOGGER.info(s"Reading from ftpServer $host source is $path and target is $HDFSTemp")

    val sftpClient = getSFTPClient(username, password, pemFileLocation, pemPassphrase, host, port, cryptoKey,cryptoAlgorithm)
    if(sftpClient.exist(path)) {
      copy(sftpClient, path,tmpFolder, copyLatest.toBoolean)
      LOGGER.info(s"Copying to HDFS from $tmpFolder to $HDFSTemp")
      FileUtils.copyToHDFS(tmpFolder, HDFSTemp)
    }
    DContainer.emptyContainer()
  }
}
