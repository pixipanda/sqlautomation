package com.pixipanda.sqlautomation.handler.transform

import com.pixipanda.sqlautomation.handler.Handler

abstract class TransformHandler(query:String, view: String) extends Handler {

  def process(): Unit

}
