package com.pixipanda.handler.transform

import com.pixipanda.Spark
import com.pixipanda.handler.Handler

abstract class TransformHandler(query:String, view: String) extends Handler with Spark {

  def process(): Unit

}
