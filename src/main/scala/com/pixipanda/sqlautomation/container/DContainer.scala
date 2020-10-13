package com.pixipanda.sqlautomation.container

import org.apache.spark.sql.DataFrame

case class DContainer(keyValue: Map[String, DataFrame], dfs: Seq[DataFrame]) {

  /*
   * This constructor will create an empty container objectt
   */
  def this() {
    this(Map[String, DataFrame](), List[DataFrame]())
  }

  /*
   * This constructor will create a container object with the given dataFrame
   */
  def this(df: DataFrame) {
    this(Map[String, DataFrame](), Seq(df))
  }

  /*
   * This constructor will create a container object with the given collection of dataFrames
   */
  def this(dfs: Seq[DataFrame]) {
    this(Map[String, DataFrame](), dfs)
  }


  /*
   * This constructor will create a container object for the given keyvalue of viewName and DataFrame
   */
  def this(keyValue: Map[String, DataFrame]) {
    this(keyValue, List[DataFrame]())
  }

  /*
   * This function will check
   */
  def isEmpty: Boolean = {
    keyValue.isEmpty && dfs.isEmpty
  }

}

object DContainer {

  /*
    This function will create a container with the given viewName and the dataFrame
  */
  def apply(viewName: Option[String], df: DataFrame): DContainer = {

    viewName match {
      case Some(view) => new DContainer(Map(view -> df))
      case None => new DContainer(df)
    }
  }

  /*
    This function will merge a given collection of containers into a single container
  */
  def mergeContainers(containers: Seq[DContainer]):DContainer = {

    val newKeyValue = containers.map(dc => dc.keyValue).reduce(_ ++ _)
    val newDFs = containers.map(dc => dc.dfs).reduce(_ ++ _)
    new DContainer(newKeyValue, newDFs)
  }

  /*
   This function will return an empty container
  */
  def emptyContainer(): DContainer = {
    new DContainer
  }
}