package com.pixipanda.sqlautomation

import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.pipeline.ETLPipeline
import com.pixipanda.sqlautomation.utils.HadoopUtils

object Main extends Spark {

  def main(args: Array[String]): Unit = {
    val fs = HadoopUtils.getFileSystem
    val etlPipeline = ETLPipeline.buildPipeline(ConfigRegistry.sqlAutomate, fs)
    etlPipeline.process()
  }
}
