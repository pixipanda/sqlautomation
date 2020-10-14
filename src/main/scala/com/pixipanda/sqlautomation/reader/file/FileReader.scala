package com.pixipanda.sqlautomation.reader.file

import java.io.File

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.config.common.SourceConfig
import com.pixipanda.sqlautomation.container.DContainer
import com.pixipanda.sqlautomation.reader.Reader
import org.apache.avro.Schema
import org.apache.log4j.Logger
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters._

/*
 FileReader class is used to read data from different files sources like CSV, XML, JSON, ORC, PARQUET, FTPSERVER etc

*/
case class FileReader(sourceConfig: SourceConfig) extends Reader with Spark{

  val LOGGER: Logger = Logger.getLogger(getClass.getName)


  /*
   * Specifying schema while reading from a source is a best practice. Please do specify the schema as Avro avsc file.
   * This avsc file is converted to Spark datatype
   */
  private def schema = {

    LOGGER.info("Parsing schema")
    sourceConfig.schemaPath match {
      case Some(schemaPath) =>
        val parser = new Schema.Parser
        val avroSchema = parser.parse(new File(schemaPath))
        val structFields = avroSchema.getFields.asScala.toList.map(field => {
          StructField(field.name, SchemaConverters.toSqlType(field.schema).dataType, nullable = true)
        })
        val structType = StructType(structFields)
        LOGGER.info(s"schema: $structType")
        Some(structType)
      case None => None
    }
  }


  /*
   * This function will read data from different file sources like
   * CSV, XML, JSON, ORC, PARQUET, FTPSERVER etc.
   * Returns: a container that has view and the dataFrame of the data read from the sources
   */
  def read:DContainer = {

    val fileOptions = sourceConfig.options

    LOGGER.info(s"Reading from ${sourceConfig.sourceType} file. Source option is $fileOptions")


    val dfReader = spark.read
      .format(fileOptions("format"))

    val df =  this.schema match {
      case Some(structType) =>
        dfReader.schema(structType)
          .options(fileOptions)
          .load(fileOptions("path"))
      case None => dfReader.options(fileOptions)
        .load(fileOptions("path"))
    }

    DContainer(sourceConfig.viewName, df, sourceConfig.viewOptions)
  }
}