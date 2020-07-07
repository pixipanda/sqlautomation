package com.pixipanda.sqlautomation.factory

import com.pixipanda.sqlautomation.constants.ETL
import org.scalatest.FunSpec

class HandlerFactorySpec extends  FunSpec{

  describe("Get LoadHandler Factory") {

    it("should create load handler factory") {

      val loadHandlerFactory = LoadHandlerFactory()
      val sut = HandlerFactory(ETL.LOAD)
      assert(sut  == loadHandlerFactory)
    }
  }

  describe("Get TransformHandler Factory") {

    it("should create transform handler factory") {

      val transformHandlerFactory = TransformHandlerFactory()
      val sut = HandlerFactory(ETL.TRANSFORM)
      assert(sut  == transformHandlerFactory)
    }
  }
}
