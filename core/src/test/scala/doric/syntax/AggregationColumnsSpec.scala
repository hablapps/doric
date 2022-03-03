package doric
package syntax

import doric.implicitConversions.stringCname
import doric.Equalities._
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum

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
        List(Some(6.5d), Some(3.0d))
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

  describe("skewness doric function") {
    import spark.implicits._

    it("should work as skewness spark function") {
      val df = List(
        ("k1", -10.0, 2.0),
        ("k1", -20.0, 3.0),
        ("k1", 100.0, 3.0),
        ("k1", 1000.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        skewness(colDouble("col1")),
        f.skewness(f.col("col1")),
        List(Some(1.1135657469022011), None)
      )
    }
  }

  describe("stdDev & stdDevSamp doric functions") {
    import spark.implicits._

    it("should work as stddev spark function") {
      val df = List(
        ("k1", 1.0, 2.0),
        ("k1", 2.0, 3.0),
        ("k1", 3.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        stdDev(colDouble("col1")),
        f.stddev(f.col("col1")),
        List(Some(1.0), None)
      )
    }

    it("should work as stddev_samp spark function") {
      val df = List(
        ("k1", 1.0, 2.0),
        ("k1", 2.0, 3.0),
        ("k1", 3.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        stdDevSamp(colDouble("col1")),
        f.stddev_samp(f.col("col1")),
        List(Some(1.0), None)
      )
    }
  }

  describe("stdDevPop doric function") {
    import spark.implicits._

    it("should work as stddev_pop spark function") {
      val df = List(
        ("k1", 1.0, 2.0),
        ("k1", 2.0, 3.0),
        ("k1", 3.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        stdDevPop(colDouble("col1")),
        f.stddev_pop(f.col("col1")),
        List(Some(0.816496580927726), Some(0.0))
      )
    }
  }

  describe("sumDistinct2Long & sumDistinct2Double doric function") {
    import spark.implicits._

    it("should work as sumDistinct spark function (long)") {
      val df = List(
        ("k1", 1L),
        ("k1", 1L),
        ("k1", 3L),
        ("k2", 6L)
      ).toDF("keyCol", "col1")

      df.testAggregation(
        "keyCol",
        sumDistinct(colLong("col1")),
        new Column(
          Sum(f.col("col1").expr).toAggregateExpression(isDistinct = true)
        ),
        List(Some(4L), Some(6L))
      )
    }

    it("should work as sumDistinct spark function (double)") {
      val df = List(
        ("k1", 1.0),
        ("k1", 1.0),
        ("k1", 3.0),
        ("k2", 6.0)
      ).toDF("keyCol", "col1")

      df.testAggregation(
        "keyCol",
        sumDistinct(colDouble("col1")),
        new Column(
          Sum(f.col("col1").expr).toAggregateExpression(isDistinct = true)
        ),
        List(Some(4.0), Some(6.0))
      )
    }
  }

  describe("variance & varSamp doric functions") {
    import spark.implicits._

    they("") {}

    it("should work as variance spark function") {
      val df = List(
        ("k1", 1.0, 2.0),
        ("k1", 2.0, 3.0),
        ("k1", 3.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        variance(colDouble("col1")),
        f.variance(f.col("col1")),
        List(Some(1.0), None)
      )
    }

    it("should work as var_samp spark function") {
      val df = List(
        ("k1", 1.0, 2.0),
        ("k1", 2.0, 3.0),
        ("k1", 3.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        varSamp(colDouble("col1")),
        f.var_samp(f.col("col1")),
        List(Some(1.0), None)
      )
    }
  }

  describe("varPop doric function") {
    import spark.implicits._

    it("should work as var_pop spark function") {
      val df = List(
        ("k1", 1.0, 2.0),
        ("k1", 2.0, 3.0),
        ("k1", 3.0, 3.0),
        ("k2", 6.0, 4.0)
      ).toDF("keyCol", "col1", "col2")

      df.testAggregation(
        "keyCol",
        varPop(colDouble("col1")),
        f.var_pop(f.col("col1")),
        List(Some(0.6666666666666666), Some(0.0))
      )
    }
  }

  describe("grouping doric function") {
    import spark.implicits._

    it("should work as grouping spark function") {
      val df = List(
        ("k1", 1),
        ("k2", 6)
      ).toDF("keyCol", "col1")

      df.cube(colString("keyCol"), colInt("col1"))
        .testGrouped(
          grouping(colInt("col1")),
          f.grouping(f.col("col1")),
          List(
            Some(0.toByte),
            Some(0.toByte),
            Some(1.toByte),
            Some(1.toByte),
            Some(0.toByte),
            Some(0.toByte),
            Some(1.toByte)
          )
        )
    }

    it("should work as grouping spark function (CName)") {
      val df = List(
        ("k1", 1),
        ("k2", 6)
      ).toDF("keyCol", "col1")

      df.cube(colString("keyCol"), colInt("col1"))
        .testGrouped(
          grouping("col1"),
          f.grouping("col1"),
          List(
            Some(0.toByte),
            Some(0.toByte),
            Some(1.toByte),
            Some(1.toByte),
            Some(0.toByte),
            Some(0.toByte),
            Some(1.toByte)
          )
        )
    }
  }

  describe("groupingId doric function") {
    import spark.implicits._

    it("should work as grouping_id spark function") {
      val df = List(
        ("k1", 1),
        ("k2", 6)
      ).toDF("keyCol", "col1")

      df.cube(colString("keyCol"), colInt("col1"))
        .testGrouped(
          groupingId(colString("keyCol"), colInt("col1")),
          f.grouping_id(f.col("keyCol"), f.col("col1")),
          List(
            Some(2L),
            Some(0L),
            Some(1L),
            Some(3L),
            Some(2L),
            Some(0L),
            Some(1L)
          )
        )
    }

    it("should work as grouping_id spark function (CName)") {
      val df = List(
        ("k1", 1),
        ("k2", 6)
      ).toDF("keyCol", "col1")

      df.cube(colString("keyCol"), colInt("col1"))
        .testGrouped(
          groupingId("keyCol", "col1"),
          f.grouping_id("keyCol", "col1"),
          List(
            Some(2L),
            Some(0L),
            Some(1L),
            Some(3L),
            Some(2L),
            Some(0L),
            Some(1L)
          )
        )
    }
  }
}
