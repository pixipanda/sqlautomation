package com.pixipanda.sqlautomation.utils

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.config.{ConfigRegistry, SourceConfig}
import com.pixipanda.sqlautomation.pipeline.Pipeline
import com.pixipanda.sqlautomation.reader.file.FileReader
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSpec

object TestUtils extends FunSpec with TestingSparkSession{


  case class Employee(emp_id: Int,
                      name:String,
                      superior_emp_id: Int,
                      emp_dept_id: Int,
                      gender: String,
                      salary: Double
                     )


/*  val empColumns: Seq[String] = Seq("emp_id","name","superior_emp_id","year_joined",
    "emp_dept_id","gender","salary")

  val employee: Seq[(Int, String, Int, String, Int, String, Double)] = List((1,"Smith",-1,"2018",10,"M",3000.0),
    (2,"Rose",1,"2010",20,"M",4000.0),
    (3,"Williams",1,"2010",10,"M",1000.0),
    (4,"Jones",2,"2005",10,"F",2000.0),
    (5,"Brown",2,"2010",40,"M",-1.0)
  )*/

  val employees: Seq[Employee] = Seq(
    Employee(1,"Smith",-1,10,"M",3000.0),
    Employee(2,"Rose",1,20,"M",4000.0),
    Employee(3,"Williams",1,10,"M",1000.0),
    Employee(4,"Jones",2,10,"F",2000.0),
    Employee(5,"Brown",2,40,"M",-1.0)
  )

  def empDF: DataFrame = {
    import spark.implicits._
    employees.toDF()
  }


  def buildPipeline(configFile: String): Pipeline[Unit, Unit] = {

    ConfigRegistry.setEnv("qa")
    ConfigRegistry.parseConfig(configFile)
    ConfigRegistry.debugAppConfig()
    Pipeline.buildPipeline(ConfigRegistry.appConfig)

  }

  def runPipeline(configFile: String): Unit = {

    buildPipeline(configFile).process()
  }


  def employeeDFToSeq(df:DataFrame): Seq[Employee] = {

    df.collect
      .toList
      .map(row => {
          val emp_id = row.getAs[Int]("emp_id")
          val name = row.getAs[String]("name")
          val superior_emp_id = row.getAs[Int]("superior_emp_id")
          val emp_dept_id = row.getAs[Int]("emp_dept_id")
          val gender = row.getAs[String]("gender")
          val salary = row.getAs[Double]("salary")
        Employee(emp_id, name, superior_emp_id, emp_dept_id, gender, salary)
      })
  }


  def readEmployee(fileSourceConfig: SourceConfig): Seq[Employee] = {
    val fileReader = FileReader(fileSourceConfig)
    val readContainer = fileReader.read

    val df = readContainer.dfs.head
    employeeDFToSeq(df)
  }


  def createEmployeeTable(): DataFrame = {

    spark.sql("CREATE DATABASE IF NOT EXISTS test_db1 LOCATION 'test_db1.db'")


    spark.sql("DROP TABLE IF EXISTS test_db1.employee")
    spark.sql("CREATE TABLE IF NOT EXISTS test_db1.employee(" +
      "emp_id int," +
      "name string," +
      "superior_emp_id int," +
      "emp_dept_id int," +
      "gender string," +
      "salary double" +
      ")" +
      "STORED AS ORC"
    )
  }

  def createDepartmentTable():DataFrame = {

    spark.sql("DROP TABLE IF EXISTS test_db1.department")
    spark.sql("CREATE TABLE IF NOT EXISTS test_db1.department(" +
      "dept_name int," +
      "dept_id int)")


    spark.sql("DROP TABLE IF EXISTS test_db1.result")
    spark.sql("CREATE TABLE IF NOT EXISTS test_db1.result(" +
      "emp_id int," +
      "name string," +
      "dept_name string)"
    )
  }
}
