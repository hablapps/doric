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
    val errorCol = "error"
    val test1    = "test2"
    it("works withColumn") {
      val test = "test"
      val result = spark
        .range(10)
        .withColumn(test, colLong("id") * 2L)

      val errors = intercept[DoricMultiError] {
        result.withColumn(
          errorCol,
          colString(errorCol).unsafeCast[Long] + colLong(test) + colLong(
            test1
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

      val errorsFilter = intercept[DoricMultiError] {
        result.filter(
          colString(errorCol).unsafeCast[Long] + colLong("id") + colLong(
            test1
          ) > 3L
        )
      }

      errorsFilter.getMessage should include("filter")
      errorsFilter.errors.length shouldBe 2

      val errorsWhere = intercept[DoricMultiError] {
        result.where(
          colString(errorCol).unsafeCast[Long] + colLong("id") + colLong(
            test1
          ) > 3L
        )
      }

      errorsWhere.getMessage should include("where")
      errorsWhere.errors.length shouldBe 2
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
      spark
        .range(10)
        .withColumns(
          "a" -> colLong("id"),
          "b" -> colLong("id"),
          "c" -> colLong("id"),
          "d" -> colLong("id"),
          "e" -> colLong("id")
        )
        .columns
        .length shouldBe 6

      val x = Map(
        "a" -> colLong("id"),
        "b" -> colLong("id"),
        "c" -> colLong("id"),
        "d" -> colLong("id"),
        "e" -> colLong("id")
      )

      spark
        .range(10)
        .withColumns(x)
        .columns
        .length shouldBe 6
    }

    it("throws an " + errorCol + " if names are repeated") {
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
      error.getMessage should startWith(
        if (!spark.version.startsWith("3.4"))
          "Found duplicate column(s) in given column names:"
        else
          "[COLUMN_ALREADY_EXISTS] The column `a` already exists. Consider to choose another name or rename the existing column."
      )
      error.getMessage should include("`a`")
      if (!spark.version.startsWith("3.4")) {
        error.getMessage should include("`b`")
      }
    }

    it("should work with 'withNamedColumns' as with 'namedColumns'") {
      val df = spark
        .range(2)
        .withNamedColumns(1.lit.as("hi"), col[Long]("id") + 10 as "id")
      df.columns shouldBe Array("id", "hi")
      df.collectCols(col[Long]("id"), col[Int]("hi")) shouldBe List(
        (10, 1),
        (11, 1)
      )
    }
  }
}
