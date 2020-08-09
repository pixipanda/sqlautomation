package com.pixipanda.sqlautomation.utils

import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.pixipanda.sqlautomation.config.common.SourceConfig
import com.pixipanda.sqlautomation.container.DContainer
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


  case class Department(dept_name: String, dept_id: Int)
  val dept: Seq[Department] = Seq(Department("Finance",10),
    Department("Marketing",20),
    Department("Sales",30),
    Department("IT",40)
  )
  def deptDF: DataFrame = {
    import  spark.implicits._
    dept.toDF()
  }


  case class EmpDept(emp_id: Int, name: String, dept_name: String)
  val empDept: Seq[EmpDept] = Seq(
    EmpDept(1, "Smith", "Finance"),
    EmpDept(2, "Rose", "Marketing"),
    EmpDept(3, "Williams", "Finance"),
    EmpDept(4, "Jones", "Finance"),
    EmpDept(5, "Brown", "IT")
  )
  def empDeptDF: DataFrame = {
    import spark.implicits._
    dept.toDF()
  }

  def buildPipeline(configFile: String): Pipeline[Unit, _ >: Unit with DContainer] = {
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

   def createDeptTable():DataFrame = {
    spark.sql("DROP TABLE IF EXISTS test_db1.department")
    spark.sql("CREATE TABLE IF NOT EXISTS test_db1.department(" +
      "dept_name String," +
      "dept_id int" +
      ")" +
      "STORED AS ORC"
    )
  }

  def createEmpJoinDeptTable(): DataFrame = {
    spark.sql("DROP TABLE IF EXISTS test_db1.empDept")
    spark.sql("CREATE TABLE IF NOT EXISTS test_db1.empDept(" +
      "emp_id int," +
      "name string," +
      "dept_name string" +
      ")" +
      "STORED AS ORC"
    )
  }

  def createEmpJoinDeptPartitionTable(): DataFrame = {
    spark.sql("DROP TABLE IF EXISTS test_db1.empDeptPartition")
    spark.sql("CREATE TABLE IF NOT EXISTS test_db1.empDeptPartition(" +
      "emp_id int," +
      "name string" +
      ")" +
      "PARTITIONED BY (dept_name string)" +
      "STORED AS ORC"
    )
  }

  def populateTable(db: String, table: String, df: DataFrame): Unit = {
    df.coalesce(1)
      .write
      .format("orc")
      .mode("overwrite")
      .insertInto(s"$db.$table")
  }
}