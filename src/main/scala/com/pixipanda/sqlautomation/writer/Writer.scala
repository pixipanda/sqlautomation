package com.pixipanda.sqlautomation.writer

import com.pixipanda.sqlautomation.container.DContainer

abstract class Writer {

  def write(container: DContainer): Unit
}
