package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.types.NullType

class StringColumns31Spec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("raiseError doric function") {
    import spark.implicits._

    val df = List("this is an error").toDF("errorMsg")

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

      doricErr.getMessage shouldBe sparkErr.getMessage
    }
  }

}
