package com.pixipanda.sqlautomation.handler.transform

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.handler.Handler

abstract class TransformHandler(query:String, view: String) extends Handler with Spark {

  def process(): Unit

}
