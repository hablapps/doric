package doric
package syntax

import org.scalatest.{Assertion, EitherValues}
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.types.NullType

class StringColumns31Spec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("raiseError doric function") {
    import spark.implicits._

    lazy val errorMsg = "this is an error"
    lazy val df       = List(errorMsg).toDF("errorMsg")

    def validateExceptions(
        doricExc: RuntimeException,
        sparkExc: RuntimeException
    ): Assertion = {
      doricExc.getMessage should fullyMatch regex
        s"""${sparkExc.getMessage}\n( )*at ${this.getClass.getSimpleName}.scala:(\\d)+"""
    }

    it("should work as spark raise_error function") {
      import java.lang.{RuntimeException => exception}

      val doricErr = intercept[exception] {
        val res = df.select(colString("errorMsg").raiseError)

        res.schema.head.dataType shouldBe NullType
        res.collect()
      }
      val sparkErr = intercept[exception] {
        df.select(f.raise_error(f.col("errorMsg"))).collect()
      }

      validateExceptions(doricErr, sparkErr)
    }

    it("should be available for strings") {
      import java.lang.{RuntimeException => exception}

      val doricErr = intercept[exception] {
        val res = df.select(raiseError(errorMsg))

        res.schema.head.dataType shouldBe NullType
        res.collect()
      }
      val sparkErr = intercept[exception] {
        df.select(f.raise_error(f.col("errorMsg"))).collect()
      }

      validateExceptions(doricErr, sparkErr)
    }
  }

}
