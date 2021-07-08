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
  }
}
