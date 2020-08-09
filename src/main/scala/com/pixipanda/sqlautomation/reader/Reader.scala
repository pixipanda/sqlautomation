package com.pixipanda.sqlautomation.reader

import com.pixipanda.sqlautomation.container.DContainer

abstract class Reader {

  def read: DContainer
}