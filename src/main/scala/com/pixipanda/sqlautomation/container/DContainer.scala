package com.pixipanda.sqlautomation.container

import org.apache.spark.sql.DataFrame

case class ViewOptions(df: DataFrame, options: Map[String, String])

case class DContainer(keyValue: Map[String, ViewOptions], dfs: Seq[DataFrame]) {

  /*
   * This constructor will create an empty container objectt
   */
  def this() {
    this(Map[String, ViewOptions](), List[DataFrame]())
  }

  /*
   * This constructor will create a container object with the given dataFrame
   */
  def this(df: DataFrame) {
    this(Map[String, ViewOptions](), Seq(df))
  }

  /*
   * This constructor will create a container object with the given collection of dataFrames
   */
  def this(dfs: Seq[DataFrame]) {
    this(Map[String, ViewOptions](), dfs)
  }


  /*
   * This constructor will create a container object for the given keyvalue of viewName and DataFrame
   */
  def this(keyValue: Map[String, ViewOptions]) {
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
    This function will create a container with the given viewName, dataFrame and the all the options related to the view
  */
  def apply(viewName: Option[String], df: DataFrame, options: Option[Map[String, String]]): DContainer = {

    viewName match {
      case Some(view) =>
        val viewOptions = options match {
          case Some(o) => ViewOptions(df, o)
          case None =>  ViewOptions(df, Map[String, String]())
        }
        new DContainer(Map(view -> viewOptions))
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