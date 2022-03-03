package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

class BooleanColumns31Spec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("assertTrue doric function") {
    import spark.implicits._

    it("should do nothing if assertion is true") {
      val df = Seq(true, true, true)
        .toDF("col1")

      df.testColumns("col1")(
        c => colBoolean(c).assertTrue,
        c => f.assert_true(f.col(c)),
        List(None, None, None)
      )
    }

    it("should throw an exception if assertion is false") {
      val df = Seq(true, false, false)
        .toDF("col1")

      intercept[java.lang.RuntimeException] {
        df.select(
          colBoolean("col1").assertTrue
        ).collect()
      }
    }

    it("should throw an exception if assertion is false with a message") {
      val df = Seq(true, false, false)
        .toDF("col1")

      val errorMessage = "this is an error message"

      val exception = intercept[java.lang.RuntimeException] {
        df.select(
          colBoolean("col1").assertTrue(errorMessage.lit)
        ).collect()
      }

      exception.getMessage shouldBe errorMessage
    }
  }

}
