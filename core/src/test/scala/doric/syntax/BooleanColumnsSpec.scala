package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

import doric.implicitConversions.stringCname

class BooleanColumnsSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("Boolean columns") {
    import spark.implicits._
    val df = Seq(Some(true), Some(false), None)
      .toDF("col1")

    object BoolF extends BooleanColumns

    it("should be inverted by not") {
      df.testColumns("col1")(
        c => BoolF.not(colBoolean(c)),
        c => f.not(f.col(c)),
        List(Some(false), Some(true), None)
      )
    }

    it("should be inverted by !") {
      df.testColumns("col1")(
        c => BoolF.!(colBoolean(c)),
        c => f.not(f.col(c)),
        List(Some(false), Some(true), None)
      )
    }
  }

  describe("and doric function") {
    import spark.implicits._

    it("should work as and spark function") {
      val df = Seq(Some(true), Some(false), None)
        .toDF("col1")

      df.testColumns2("col1", true)(
        (c, b) => colBoolean(c) and lit(b),
        (c, b) => f.col(c) and f.lit(b),
        List(Some(true), Some(false), None)
      )
    }

    it("should be the same as && doric function") {
      val df = Seq(Some(true), Some(false), None)
        .toDF("col1")

      val res = df.select(
        colBoolean("col1") and lit(true) as "dcol",
        colBoolean("col1") && lit(true) as "scol"
      )

      compareDifferences(res, List(Some(true), Some(false), None))
    }
  }

  describe("or doric function") {
    import spark.implicits._

    it("should work as or spark function") {
      val df = Seq(Some(true), Some(false), None)
        .toDF("col1")

      df.testColumns2("col1", true)(
        (c, b) => colBoolean(c) or lit(b),
        (c, b) => f.col(c) or f.lit(b),
        List(Some(true), Some(true), Some(true))
      )
    }

    it("should be the same as || doric function") {
      val df = Seq(Some(true), Some(false), None)
        .toDF("col1")

      val res = df.select(
        colBoolean("col1") or lit(true) as "dcol",
        colBoolean("col1") || lit(true) as "scol"
      )

      compareDifferences(res, List(Some(true), Some(true), Some(true)))
    }
  }

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
