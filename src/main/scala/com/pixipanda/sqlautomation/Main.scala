package com.pixipanda.sqlautomation

import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.pipeline.ETLPipeline


object Main extends Spark {
  def main(args: Array[String]): Unit = {
    val env = args(0)
    ConfigRegistry.setEnv(env)
    ConfigRegistry.parseConfig()
    val etlPipeline = ETLPipeline.buildPipeline(ConfigRegistry.sqlAutomate)
    etlPipeline.process()
  }
}
