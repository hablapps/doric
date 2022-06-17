package doric
package syntax

import doric.implicitConversions.stringCname
import doric.Equalities._
import java.sql.Date
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

class AggregationColumns31Spec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("percentileApprox doric function") {
    import spark.implicits._
    val df = List(
      ("k1", 0.0),
      ("k1", 1.0),
      ("k1", 2.0),
      ("k1", 10.0)
    ).toDF("keyCol", "col1")
    val dfDate = List(
      ("k1", Date.valueOf("2021-12-05")),
      ("k1", Date.valueOf("2021-12-06")),
      ("k1", Date.valueOf("2021-12-07")),
      ("k1", Date.valueOf("2021-12-01"))
    ).toDF("keyCol", "col1")

    it(
      "should work as spark percentile_approx function working with percentile array & double type"
    ) {
      val percentage = Array(0.5, 0.4, 0.1)
      val accuracy   = 100
      df.testAggregation(
        "keyCol",
        percentileApprox(
          colDouble("col1"),
          percentage,
          accuracy
        ),
        f.percentile_approx(
          f.col("col1"),
          f.lit(percentage),
          f.lit(accuracy)
        ),
        List(Some(Array(1.0, 1.0, 0.0)))
      )
    }

    it(
      "should work as spark percentile_approx function working with percentile array & date type"
    ) {
      val percentage = Array(0.5, 0.4, 0.1)
      val accuracy   = 100

      dfDate.testAggregation(
        "keyCol",
        percentileApprox(
          colDate("col1"),
          percentage,
          accuracy
        ),
        f.percentile_approx(
          f.col("col1"),
          f.lit(percentage),
          f.lit(accuracy)
        ),
        List(
          Some(
            Array(
              Date.valueOf("2021-12-05"),
              Date.valueOf("2021-12-05"),
              Date.valueOf("2021-12-01")
            )
          )
        )
      )
    }

    it("should throw an exception if percentile array & wrong percentile") {
      val msg = intercept[java.lang.IllegalArgumentException] {
        df.select(
          percentileApprox(colDouble("col1"), Array(-0.5, 0.4, 0.1), 100)
        ).collect()
      }

      msg.getMessage shouldBe "requirement failed: Each value of percentage must be between 0.0 and 1.0."
    }

    it("should throw an exception if percentile array & wrong accuracy") {
      val msg = intercept[java.lang.IllegalArgumentException] {
        df.select(
          percentileApprox(colDouble("col1"), Array(0.5, 0.4, 0.1), -1)
        ).collect()
      }

      msg.getMessage shouldBe s"requirement failed: The accuracy provided must be a literal between (0, ${Int.MaxValue}] (current value = -1)"
    }

    it(
      "should work as spark percentile_approx function working with percentile double & double type"
    ) {
      val percentage = 0.5
      val accuracy   = 100

      df.testAggregation(
        "keyCol",
        percentileApprox(colDouble("col1"), percentage, accuracy),
        f.percentile_approx(f.col("col1"), f.lit(percentage), f.lit(accuracy)),
        List(Some(1.0))
      )
    }

    it(
      "should work as spark percentile_approx function working percentile double & date type"
    ) {
      val percentage = 0.5
      val accuracy   = 100

      dfDate.testAggregation(
        "keyCol",
        percentileApprox(
          colDate("col1"),
          percentage,
          accuracy
        ),
        f.percentile_approx(
          f.col("col1"),
          f.lit(percentage),
          f.lit(accuracy)
        ),
        List(Some(Date.valueOf("2021-12-05")))
      )
    }

    it("should throw an exception if percentile double & wrong percentile") {
      val msg = intercept[java.lang.IllegalArgumentException] {
        df.select(
          percentileApprox(colDouble("col1"), -0.5, 100)
        ).collect()
      }

      msg.getMessage shouldBe "requirement failed: Percentage must be between 0.0 and 1.0."
    }

    it("should throw an exception if percentile double & wrong accuracy") {
      val msg = intercept[java.lang.IllegalArgumentException] {
        df.select(
          percentileApprox(colDouble("col1"), 0.5, -1)
        ).collect()
      }

      msg.getMessage shouldBe s"requirement failed: The accuracy provided must be a literal between (0, ${Int.MaxValue}] (current value = -1)"
    }
  }

}
