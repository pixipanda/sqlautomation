package com.pixipanda.sqlautomation.utils

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

object FileUtils {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def createConfiguration: Configuration = {
    val hadoopConf = new Configuration()
    hadoopConf.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    hadoopConf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))
    hadoopConf
  }


  def getFileSystem: FileSystem = {
    val conf = createConfiguration
    FileSystem.get(conf)
  }


  def getFileSystem(configuration: Configuration): FileSystem = {
    FileSystem.get(configuration)
  }


  def renamePartFile(conf: Configuration, path: String, fileName: String):Unit = {

    logger.debug(s"path: $path fileName: $fileName")

    val fs = FileSystem.get(conf)
    val file = new Path(s"$path/$fileName")
    if(fs.exists(file)) {
      logger.debug(s"Deleting the file: $file")
      fs.delete(new Path(s"$path/$fileName"), true)
    }

    val partFileName = fs.globStatus(new Path(s"$path/part*"))(0).getPath.getName
    val result = fs.rename(new Path(s"$path/$partFileName"), file)
    if(result) {
      logger.info("Successfully renamed")
    } else {
      logger.error("Failed to rename")
    }
  }


  def copyFilesToDir(srcLocalDir: String, destHDFSDir: String, fs: FileSystem) {
    println(s"srcDirPath: $srcLocalDir destDirPath: $destHDFSDir")
    val dir = new File(srcLocalDir)
    if(dir.exists()) {
      dir.listFiles().foreach(file => {
        println(s"fs schema: ${fs.getScheme}")
        fs.copyFromLocalFile(new Path(file.getPath), new Path(destHDFSDir, file.getName))
      })
    }else {
      logger.error(s"local dir $srcLocalDir do not exist")
    }
  }

  def copyToHDFS(localTempFileLocation : String, HDFSTempLocation : String): Unit  = {
    println(s"localTempFileLocation: $localTempFileLocation HDFSTempLocation: $HDFSTempLocation")
    val fs = FileUtils.getFileSystem(FileUtils.createConfiguration)
    copyFilesToDir(localTempFileLocation, HDFSTempLocation, fs)
  }
}
