package com.pixipanda.sqlautomation.container

import com.pixipanda.sqlautomation.TestingSparkSession
import org.scalatest.FunSpec

class ContainerSpec extends FunSpec with TestingSparkSession{

  describe("Container Testing Spec") {

    describe("Functionality") {

      val df1 = spark.emptyDataFrame
      val df2 = spark.emptyDataFrame
      val viewName1 = "view1"
      val viewName2 = "view2"


      it("should create empty container") {

        val sut = DContainer()
        assert(sut.dfs.isEmpty)
        assert(sut.keyValue.isEmpty)
      }

      it("should create container from the given view and dataframe") {
        val sut = DContainer(Some(viewName1), df1)
        assert(!sut.isEmpty)
        assert(sut.keyValue.size == 1)
        assert(sut.dfs.isEmpty)
      }

      it("should create container without view") {

        val sut = DContainer(df1)
        assert(!sut.isEmpty)
        assert(sut.dfs.size == 1)
        assert(sut.keyValue.isEmpty)
      }

      it("should merge container") {

        val containers = List(DContainer(Some(viewName1), df1), DContainer(Some(viewName2), df2))
        val sut = DContainer.mergeContainers(containers)
        assert(!sut.isEmpty)
        assert(sut.keyValue.size == 2)
        assert(sut.dfs.isEmpty)

      }
    }
  }

}