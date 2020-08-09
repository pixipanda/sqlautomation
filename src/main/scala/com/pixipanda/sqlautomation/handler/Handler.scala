package com.pixipanda.sqlautomation.handler

trait Handler[I, O] {

  def process(input: I): O

}


