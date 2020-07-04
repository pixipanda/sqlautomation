package com.pixipanda.sqlautomation.pipeline

import java.io.InputStreamReader

import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.factory.HandlerFactory
import com.pixipanda.sqlautomation.handler.Handler
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer

class ETLPipeline {

  private val handlers = ListBuffer[Handler]()

  def addHandlers(handler: Handler): Unit = {

    handlers.append(handler)
  }

  def process(): Unit = {
    handlers.foreach(_.process())
  }
}

object ETLPipeline {

  def buildPipeline(): ETLPipeline = {

    val etlPipeline = new ETLPipeline

    val hadoopConfDir: String = System.getenv("HADOOP_CONF_DIR")
    val hadoopConf: Configuration = new Configuration()
    hadoopConf.addResource(new Path("file:///" + hadoopConfDir + "/core-site.xml"))
    hadoopConf.addResource(new Path("file:///" + hadoopConfDir + "/hdfs-site.xml"))
    val hdfs: FileSystem = FileSystem.get(hadoopConf)

    ConfigRegistry.sqlAutomate.sqlConfigs.foreach(sqlConfig => {
      val reader = new InputStreamReader(hdfs.open(new Path(sqlConfig.fileName)))
      val config: Config = ConfigFactory.parseReader(reader)
      sqlConfig.queryConfigs.foreach(queryConfig => {
        val handlerFactory = HandlerFactory(queryConfig)
        val handler = handlerFactory.getHandler(config, queryConfig)
        etlPipeline.addHandlers(handler)
      })
    })
    etlPipeline
  }
}
