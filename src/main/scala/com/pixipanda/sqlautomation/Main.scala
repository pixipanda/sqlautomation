package com.pixipanda.sqlautomation

import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.pipeline.Pipeline


object Main extends Spark {
  def main(args: Array[String]): Unit = {
    val env = args(0)
    ConfigRegistry.setEnv(env)
    ConfigRegistry.parseConfig()
    ConfigRegistry.debugAppConfig()
    val pipeline = Pipeline.buildPipeline(ConfigRegistry.appConfig)
    pipeline.process()
  }
}
