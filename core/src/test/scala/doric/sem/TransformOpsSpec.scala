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
    val errorCol = "error".cname
    val test1    = "test2".cname
    it("works withColumn") {
      val test = "test".cname
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

      val errors = intercept[DoricMultiError] {
        result.filter(
          colString(errorCol).unsafeCast[Long] + colLong("id") + colLong(
            test1
          ) > 3L
        )
      }

      errors.errors.length shouldBe 2
    }

    it("works select") {
      val result = spark
        .range(10)
        .select(
          colLong("id") > 2L as "mayor".cname,
          colLong("id").cast[String] as "casted".cname,
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
          "a".cname -> colLong("id"),
          "b".cname -> colLong("id"),
          "c".cname -> colLong("id"),
          "d".cname -> colLong("id"),
          "e".cname -> colLong("id")
        )
        .columns

      val x = Map(
        "a".cname -> colLong("id"),
        "b".cname -> colLong("id"),
        "c".cname -> colLong("id"),
        "d".cname -> colLong("id"),
        "e".cname -> colLong("id")
      )

      val colNames2 = spark
        .range(10)
        .withColumns(x)
        .columns
      colNames2.length shouldBe 6
    }

    it("throws an " + errorCol + " if names are repeated") {
      val error = intercept[Exception] {
        spark
          .range(10)
          .withColumns(
            "a".cname -> colLong("id"),
            "a".cname -> colLong("id"),
            "b".cname -> colLong("id"),
            "b".cname -> colLong("id")
          )
      }
      error.getMessage shouldBe "Found duplicate column(s) in given column names: `a`, `b`"
    }
  }
}
