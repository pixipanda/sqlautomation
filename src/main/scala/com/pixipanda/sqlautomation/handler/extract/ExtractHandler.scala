package com.pixipanda.sqlautomation.handler.extract

import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.handler.Handler
import com.pixipanda.sqlautomation.reader.Reader


case class ExtractHandler(readers: Seq[Reader])  extends Handler[Unit, DContainer] {

  override def process(input: Unit): DContainer = {

    val containers = readers.map(_.read)
    DContainer.mergeContainers(containers)

  }

}
