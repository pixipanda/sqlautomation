package com.pixipanda.sqlautomation.handler.load

import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.handler.Handler
import com.pixipanda.sqlautomation.writer.Writer


case class LoadHandler(writers: Seq[Writer]) extends Handler[DContainer, Unit]{

  override def process(input: DContainer): Unit = {
    writers.foreach(_.write(input))
  }

}
