package com.pixipanda.sqlautomation.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HadoopUtils {


  val HADOOP_CONF_DIR = "HADOOP_CONF_DIR"

  def createConfiguration: Configuration = {

    val hadoopConf = new Configuration()
    val hadoopConfDir: String = System.getenv(HADOOP_CONF_DIR)
    hadoopConf.addResource(new Path("file:///" + hadoopConfDir + "/core-site.xml"))
    hadoopConf.addResource(new Path("file:///" + hadoopConfDir + "/hdfs-site.xml"))
    hadoopConf
  }


  def getFileSystem: FileSystem = {
    val conf = createConfiguration
    FileSystem.get(conf)
  }
}
