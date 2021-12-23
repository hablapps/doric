package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{functions => f}
import doric.implicitConversions.stringCname

import java.sql.Date

class AggregationColumnsSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("sum doric function") {
    import spark.implicits._

    it("should work as spark sum function for integers") {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        sum(colInt("col1")),
        f.sum("col1"),
        List(Some(6L), Some(3L))
      )
    }

    it("should work as spark sum function for decimal numbers") {
      val df = List(
        ("k1", 1.4f, 2),
        ("k1", 5.1f, 2),
        ("k2", 3.0f, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        sum(colFloat("col1")),
        f.sum("col1"),
        List(Some(6.5), Some(3.0))
      )
    }
  }

  describe("count doric function") {
    import spark.implicits._

    it("should work as spark count function with column") {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        count(colInt("col1")),
        f.count(f.col("col1")),
        List(Some(2L), Some(1L))
      )
    }

    it("should work as spark count function with CName") {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        count("col1"),
        f.count("col1"),
        List(Some(2L), Some(1L))
      )
    }
  }

  describe("first doric function") {
    import spark.implicits._

    it("should work as spark first function without ignoring nulls") {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        first(colInt("col1")),
        f.first(f.col("col1")),
        List(Some(1), Some(3))
      )
    }

    it("should work as spark first function ignoring nulls") {
      val df = List(
        ("k1", None, 2),
        ("k1", Some(5), 2),
        ("k2", Some(3), 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        first(colInt("col1"), ignoreNulls = true),
        f.first(f.col("col1"), ignoreNulls = true),
        List(Some(5), Some(3))
      )
    }

    it("should work as spark first function NOT ignoring nulls") {
      val df = List(
        ("k1", None, 2),
        ("k1", Some(5), 2),
        ("k2", Some(3), 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        first(colInt("col1"), ignoreNulls = false),
        f.first(f.col("col1"), ignoreNulls = false),
        List(None, Some(3))
      )
    }
  }

  describe("last doric function") {
    import spark.implicits._

    it("should work as spark last function without ignoring nulls") {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        last(colInt("col1")),
        f.last(f.col("col1")),
        List(Some(5), Some(3))
      )
    }

    it("should work as spark last function ignoring nulls") {
      val df = List(
        ("k1", Some(1), 2),
        ("k1", None, 2),
        ("k2", Some(3), 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        last(colInt("col1"), ignoreNulls = true),
        f.last(f.col("col1"), ignoreNulls = true),
        List(Some(1), Some(3))
      )
    }

    it("should work as spark last function NOT ignoring nulls") {
      val df = List(
        ("k1", Some(1), 2),
        ("k1", None, 2),
        ("k2", Some(3), 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        last(colInt("col1"), ignoreNulls = false),
        f.last(f.col("col1"), ignoreNulls = false),
        List(None, Some(3))
      )
    }
  }

  describe("aproxCountDistinct doric function") {
    import spark.implicits._

    it("should work as spark approx_count_distinct function using CName") {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        aproxCountDistinct("col1"),
        f.approx_count_distinct("col1"),
        List(Some(2L), Some(1L))
      )
    }

    it(
      "should work as spark approx_count_distinct function using CName with rsd param"
    ) {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        aproxCountDistinct("col1", 0.05),
        f.approx_count_distinct("col1", 0.05),
        List(Some(2L), Some(1L))
      )
    }

    it("should work as spark approx_count_distinct function column") {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        aproxCountDistinct(colInt("col1")),
        f.approx_count_distinct(f.col("col1")),
        List(Some(2L), Some(1L))
      )
    }

    it(
      "should work as spark approx_count_distinct function column with rsd param"
    ) {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        aproxCountDistinct(colInt("col1"), 0.05),
        f.approx_count_distinct(f.col("col1"), 0.05),
        List(Some(2L), Some(1L))
      )
    }
  }

  describe("avg doric function") {
    import spark.implicits._

    it("should work as spark avg function") {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        avg(colInt("col1")),
        f.avg(f.col("col1")),
        List(Some(3.0), Some(3.0))
      )
    }
  }

  describe("collectList doric function") {
    import spark.implicits._

    it("should work as spark collect_list function") {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        collectList(colInt("col1")),
        f.collect_list(f.col("col1")),
        List(Some(Array(1, 5)), Some(Array(3)))
      )
    }
  }

  describe("collectSet doric function") {
    import spark.implicits._

    it("should work as spark collect_set function") {
      val df = List(
        ("k1", 1, 2),
        ("k1", 5, 2),
        ("k2", 3, 2),
        ("k2", 3, 2)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        collectSet(colInt("col1")),
        f.collect_set(f.col("col1")),
        List(Some(Array(1, 5)), Some(Array(3)))
      )
    }
  }

  describe("correlation doric function") {
    import spark.implicits._

    it("should work as spark corr function") {
      val df = List(
        ("k1", 3.0, 2.0),
        ("k1", 3.0, 3.0),
        ("k1", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        correlation(colDouble("col1"), colDouble("col2")),
        f.corr(f.col("col1"), f.col("col2")),
        List(Some(0.8660254037844387))
      )
    }
  }

  describe("countDistinct doric function") {
    import spark.implicits._

    it("should work as spark countDistinct function using columns") {
      val df = List(
        ("k1", 2.0, "1.0"),
        ("k1", 3.0, "2.0"),
        ("k1", 4.0, "3.0"),
        ("k1", 4.0, "3.0"),
        ("k2", 6.0, "4.0")
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        countDistinct(colDouble("col1"), colString("col2")),
        f.countDistinct(f.col("col1"), f.col("col2")),
        List(Some(3L), Some(1L))
      )
    }

    it("should work as spark countDistinct function using CNames") {
      val df = List(
        ("k1", 3.0, "2.0"),
        ("k1", 4.0, "3.0"),
        ("k2", 6.0, "4.0")
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        countDistinct("col1", "col2"),
        f.countDistinct("col1", "col2"),
        List(Some(2L), Some(1L))
      )
    }
  }

  describe("covarPop doric function") {
    import spark.implicits._

    it("should work as spark covar_pop function") {
      val df = List(
        ("k1", 3.0, 2.0),
        ("k1", 4.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        covarPop(colDouble("col1"), colDouble("col2")),
        f.covar_pop(f.col("col1"), f.col("col2")),
        List(Some(0.0), Some(0.25))
      )
    }
  }

  describe("covarSamp doric function") {
    import spark.implicits._

    it("should work as spark covar_samp function") {
      val df = List(
        ("k1", 3.0, 2.0),
        ("k1", 4.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        covarSamp(colDouble("col1"), colDouble("col2")),
        f.covar_samp(f.col("col1"), f.col("col2")),
        List(None, Some(0.5))
      )
    }
  }

  describe("kurtosis doric function") {
    import spark.implicits._

    it("should work as spark kurtosis function") {
      val df = List(
        ("k1", 3.0, 2.0),
        ("k1", 4.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        kurtosis(colDouble("col1")),
        f.kurtosis(f.col("col1")),
        List(None, Some(-2.0))
      )
    }
  }

  describe("max doric function") {
    import spark.implicits._

    it("should work as spark max function") {
      val df = List(
        ("k1", 3.0, 2.0),
        ("k1", 4.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        max(colDouble("col1")),
        f.max(f.col("col1")),
        List(Some(4.0), Some(6.0))
      )
    }
  }

  describe("min doric function") {
    import spark.implicits._

    it("should work as spark min function") {
      val df = List(
        ("k1", 3.0, 2.0),
        ("k1", 4.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        min(colDouble("col1")),
        f.min(f.col("col1")),
        List(Some(3.0), Some(6.0))
      )
    }
  }

  describe("mean doric function") {
    import spark.implicits._

    it("should work as spark mean function") {
      val df = List(
        ("k1", 3.0, 2.0),
        ("k1", 4.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        mean(colDouble("col1")),
        f.mean(f.col("col1")),
        List(Some(3.5), Some(6.0))
      )
    }
  }

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
