package com.pixipanda.sqlautomation.handler.transform

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.container.DContainer
import org.apache.spark.sql.DataFrame

case class SQLTransformHandler(query: String, viewName: Option[String])
  extends TransformHandler[DContainer, DContainer](query, viewName)
  with Spark {

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
    val df = spark.sql(query)
    DContainer(viewName, df)
  }

}
