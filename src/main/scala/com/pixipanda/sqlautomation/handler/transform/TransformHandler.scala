package com.pixipanda.sqlautomation.handler.transform

import com.pixipanda.sqlautomation.handler.Handler

//abstract class TransformHandler(query:String, view: String) extends Handler {
abstract class TransformHandler[I, O](query:String, view: Option[String]) extends Handler[I, O] {


}
