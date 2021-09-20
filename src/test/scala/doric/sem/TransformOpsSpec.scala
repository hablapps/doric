package doric
package sem

import doric.implicitConversions._
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues

class TransformOpsSpec
    extends DoricTestElements
    with Matchers
    with EitherValues {
  describe("Dataframe transformation methods") {
    it("works withColumn") {
      val result = spark
        .range(10)
        .withColumn("test", colLong("id") * 2L)

      val errors = intercept[DoricMultiError] {
        result.withColumn(
          "error",
          colString("error").unsafeCast[Long] + colLong("test") + colLong(
            "test2"
          )
        )
      }

      errors.errors.length shouldBe 2
    }

    it("works filter") {
      val result = spark
        .range(10)
        .toDF()
        .filter(colLong("id") > 2L)

      val errors = intercept[DoricMultiError] {
        result.filter(
          colString("error").unsafeCast[Long] + colLong("id") + colLong(
            "test2"
          ) > 3L
        )
      }

      errors.errors.length shouldBe 2
    }

    it("works select") {
      val result = spark
        .range(10)
        .select(
          colLong("id") > 2L as "mayor",
          colLong("id").cast[String] as "casted",
          colLong("id")
        )

      val errors = intercept[DoricMultiError] {
        result.select(
          colInt("id"),
          colLong("id") + colLong("id"),
          colLong("id2") + colLong("id3")
        )
      }

      errors.errors.length shouldBe 3
    }

    it("accepts multiple withColumns") {
      val colNames = spark
        .range(10)
        .withColumns(
          "a" -> colLong("id"),
          "b" -> colLong("id"),
          "c" -> colLong("id"),
          "d" -> colLong("id"),
          "e" -> colLong("id")
        )
        .columns

      val x = Map(
        "a" -> colLong("id"),
        "b" -> colLong("id"),
        "c" -> colLong("id"),
        "d" -> colLong("id"),
        "e" -> colLong("id")
      )

      val colNames2 = spark
        .range(10)
        .withColumns(x)
        .columns
      colNames2.length shouldBe 6
    }

    it("throws an error if names are repeated") {
      val error = intercept[Exception] {
        spark
          .range(10)
          .withColumns(
            "a" -> colLong("id"),
            "a" -> colLong("id"),
            "b" -> colLong("id"),
            "b" -> colLong("id")
          )
      }
      error.getMessage shouldBe "Found duplicate column(s) in given column names: `a`, `b`"
    }
  }
}
