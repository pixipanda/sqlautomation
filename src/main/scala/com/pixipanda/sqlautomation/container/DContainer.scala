package com.pixipanda.sqlautomation.container

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class DContainer() {

  var keyValue: Map[String, DataFrame] =  Map().empty
  var dfs: Seq[DataFrame] = List()


  def isEmpty: Boolean = {
    keyValue.isEmpty && dfs.isEmpty
  }

}

object DContainer {

  /*
     This function will create empty container
  */
  def apply(): DContainer = new DContainer()


  /*
   This function will create a container with the given dataFrame
  */
  def apply(df: DataFrame): DContainer = {

    val container = new DContainer()
    container.dfs = Seq(df)
    container
  }

  /*
    This function will create a container with the given viewName and the dataFrame
  */
  def apply(viewName: Option[String], df: DataFrame): DContainer = {

    val container = new DContainer()
    viewName match {
      case Some(view) => container.keyValue = Map(view -> df)
      case None => container.dfs = List(df)
    }
    container
  }

  /*
    This function will create a container from the keyValues and collection of dataFrames
  */
  private def apply(keyValue: Map[String, DataFrame], dfs: Seq[DataFrame]) = {

    val container = new DContainer()
    container.keyValue = keyValue
    container.dfs = dfs
    container
  }


  /*
    This function will merge a given collection of containers into a single container
  */
  def mergeContainers(containers: Seq[DContainer]):DContainer = {

    val newKeyValue = mutable.Map[String, DataFrame]()
    val newDfs = mutable.ListBuffer[DataFrame]()

    containers.foreach(container => {
      newDfs.append(container.dfs: _*)
      container.keyValue.foreach{
        case(key, value) => newKeyValue.put(key, value)
      }
    })

    DContainer(newKeyValue.toMap, newDfs.toList)
  }


  /*
   This function will return an empty container
  */
  def emptyContainer(): DContainer = {
    new DContainer
  }
}