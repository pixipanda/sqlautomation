package com.pixipanda.sqlautomation.handler.transform

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.container.DContainer
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

case class QueryTransformHandler(query: String, viewName: Option[String])
  extends TransformHandler[DContainer, DContainer](query, viewName)
    with Spark {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def createViews(container: DContainer): Unit = {
    container.keyValue.foreach {
      case (viewName: String, df: DataFrame) =>
        df.createOrReplaceTempView(viewName)
    }
  }

  override def process(input:DContainer): DContainer = {

    if(!input.isEmpty) {
      createViews(input)
    }
    logger.info(s"query: $query viewName: $viewName")
    val df = spark.sql(query)
    DContainer(viewName, df)
  }

}