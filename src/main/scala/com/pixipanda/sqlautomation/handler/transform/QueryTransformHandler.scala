package com.pixipanda.sqlautomation.handler.transform

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.container.{DContainer, ViewOptions}
import org.slf4j.{Logger, LoggerFactory}

case class QueryTransformHandler(query: String, viewName: Option[String], viewOptions: Option[Map[String, String]])
  extends TransformHandler[DContainer, DContainer](query, viewName)
    with Spark {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /*
   *  Here we are creating a view for the dataframe got from the previous query handler object
   */
  private def createViews(container: DContainer): Unit = {

    LOGGER.info("Creating views")
    container.keyValue.foreach {
      case (view: String, viewOptions: ViewOptions) =>
        viewOptions.df.createOrReplaceTempView(view)
        persistView(view, viewOptions.options.get("cache"))
    }
  }

  /*
   * This function will cache the given view. Note this view is of the previous query handler's dataFrame
   */
  private def persistView(view: String, cacheOption: Option[String]): Unit = {

    LOGGER.info(s"Persisting view $view cacheOption: $cacheOption")

    cacheOption match {
      case Some(_) => sqlContext.cacheTable(view)
        LOGGER.info(s"View $view cached")
      case None => LOGGER.info("Cache not specified in the config")
    }
  }


  override def process(input:DContainer): DContainer = {

    if(!input.isEmpty) {
      createViews(input)
    }

    LOGGER.info(s"query: $query viewName: $viewName viewOptions: $viewOptions")

    val df = spark.sql(query)
    DContainer(viewName, df, viewOptions)
  }

}