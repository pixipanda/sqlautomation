/*
package com.pixipanda.sqlautomation.pipeline


import com.pixipanda.sqlautomation.TestingSparkSession
import com.pixipanda.sqlautomation.config.{ConfigRegistry, SQLAutomate}
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class PipelineProcessSpec extends FunSpec with TestingSparkSession with BeforeAndAfterAll{

  override protected def beforeAll(): Unit = {
    createTable()
    populateTables()
  }

  def createTable():DataFrame = {

    spark.sql("CREATE DATABASE IF NOT EXISTS test_db1 LOCATION 'test_db1.db'")


    spark.sql("DROP TABLE IF EXISTS test_db1.employee")
    spark.sql("CREATE TABLE IF NOT EXISTS test_db1.employee(" +
      "emp_id int," +
      "name string," +
      "superior_emp_id int," +
      "year_joined string," +
      "emp_dept_id int," +
      "gender string," +
      "salary double)"
    )


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


  def populateTables(): Unit = {

    import spark.implicits._
    val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
      (2,"Rose",1,"2010","20","M",4000),
      (3,"Williams",1,"2010","10","M",1000),
      (4,"Jones",2,"2005","10","F",2000),
      (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1)
    )
    val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
      "emp_dept_id","gender","salary")
    val empDF = emp.toDF(empColumns:_*)
    empDF.write.format("csv").mode("overwrite").saveAsTable("test_db1.employee")

    val dept = Seq(("Finance",10),
      ("Marketing",20),
      ("Sales",30),
      ("IT",40)
    )

    val deptColumns = Seq("dept_name","dept_id")
    val deptDF = dept.toDF(deptColumns:_*)
    deptDF.write.format("csv").mode("overwrite").saveAsTable("test_db1.department")
  }


  describe("Pipeline processing") {

    import spark.implicits._

    it("should populate test_db1.result table") {

      val expectedResult = List((1, "Smith", "Finance"),
        (2, "Rose", "Marketing"),
        (3, "Williams", "Finance"),
        (4, "Jones", "Finance"),
        (5, "Brown", "IT")
      )
     /* val empDepColumns = Seq("emp_id", "name", "dept_name")
      val empDepDF = empDep.toDF(empDepColumns: _*)
      val expectedResult = empDepDF.collect().toList*/

      ConfigRegistry.setEnv("qa")
      ConfigRegistry.parseConfig("src/test/resources/job1/job1.conf")
      val sqlAutomate = SQLAutomate.parseSQLAutomate(ConfigRegistry.getConfig)
      val etlPipeline = ETLPipeline.buildPipeline(sqlAutomate)
      etlPipeline.process()
      val sut = spark.sql("SELECT * FROM test_db1.result")
        .collect()
        .toList
        .map(row => {
        (row.getAs[Int]("emp_id"),row.getAs[String]("name"), row.getAs[String]("dept_name"))
      })
      assert(sut.sortBy(_._1) == expectedResult.sortBy(_._1))
    }

    /*it("should create partition") {
      val expectedResult = List("dept_name=Finance", "dept_name=IT", "dept_name=Marketing")
      ConfigRegistry.setEnv("qa")
      ConfigRegistry.parseConfig("src/test/resources/jobx/jobx.conf")
      val sqlAutomate = SQLAutomate.parseSQLAutomate(ConfigRegistry.getConfig)
      val etlPipeline = ETLPipeline.buildPipeline(sqlAutomate)
      etlPipeline.process()
      val sut = spark.sql("SHOW PARTITIONS test_db1.result").collect().toList.map(_.getAs[String]("partition"))
      assert(sut == expectedResult)
    }*/
  }
}
*/
