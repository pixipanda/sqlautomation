package com.pixipanda.sqlautomation.handler.extract

import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.handler.Handler

class EmptyExtractHandler extends Handler[Unit, DContainer]{

  override def process(input: Unit): DContainer = {
    DContainer.emptyContainer()
  }
}
