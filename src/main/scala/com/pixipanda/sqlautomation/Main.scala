package com.pixipanda.sqlautomation

import com.pixipanda.sqlautomation.pipeline.ETLPipeline

object Main extends Spark {

  def main(args: Array[String]): Unit = {

    val etlPipeline = ETLPipeline.buildPipeline()
    etlPipeline.process()
  }
}
