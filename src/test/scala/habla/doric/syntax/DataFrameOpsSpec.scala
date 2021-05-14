package habla.doric
package syntax

import habla.doric.implicitConversions._
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues

class DataFrameOpsSpec
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

    val left = spark
      .range(10)
      .toDF()
      .withColumn(
        "otherColumn",
        functions.concat(colLong("id").cast[String], "left")
      )
      .toDF()
      .filter(colLong("id") > 3L)
    val right = spark
      .range(10)
      .toDF()
      .withColumn(
        "otherColumn",
        functions.concat(colLong("id").cast[String], "right")
      )
      .filter(colLong("id") < 7L)

    it("works with join of same name and type columns") {
      val badRight = right
        .withColumn("id", colLong("id").cast[String])

      left.join(right, "left", colLong("id"))
      left.join(right, "right", colLong("id"))
      left.join(right, "inner", colLong("id"))
      left.join(right, "outer", colLong("id"))

      val errors = intercept[DoricMultiError] {
        left.join(badRight, "left", colLong("id"))
      }

      errors.errors.length shouldBe 1
      errors.errors.head.message shouldBe "The column with name 'id' is of type StringType and it was expected to be LongType"
    }

    it("should join typesafety") {

      val joinFunction: DoricJoinColumn =
        LeftDF.colLong("id") === RightDF.colLong("id")

      left.join(right, joinFunction, "inner")

      val badJoinFunction: DoricJoinColumn =
        LeftDF.colString("id") ===
          RightDF.colString("identifier")

      val errors = intercept[DoricMultiError] {
        left.join(right, badJoinFunction, "inner")
      }

      errors.errors.length shouldBe 2
      errors.errors.head.message shouldBe "The column with name 'id' is of type LongType and it was expected to be StringType"
      errors.errors.toChain
        .get(1)
        .get
        .message shouldBe "Cannot resolve column name \"identifier\" among (id, otherColumn)"
    }

    it("should prevent key ambiguity with innerJoinDropRightKey") {
      val resultDF = left.innerJoinKeepLeftKeys(right, colLong("id"))

      resultDF.withColumn("keyAsString", colLong("id").cast[String])
      resultDF.schema.length shouldBe 3
    }

    it("should prevent non key ambiguity using colFromDf") {
      val resultDF = left.innerJoinKeepLeftKeys(right, colLong("id"))

      import spark.implicits._

      resultDF
        .withColumn("nonKeyColRight", colFromDF[String]("otherColumn", right))
        .withColumn("nonKeyColLeft", colFromDF[String]("otherColumn", left))
        .collectCols(
          (colString(
            "nonKeyColRight"
          ) === colFromDF[String]("otherColumn", right)) && (colString(
            "nonKeyColLeft"
          ) === colFromDF[String]("otherColumn", left))
        )
        .forall(identity) shouldBe true
      resultDF.schema.length shouldBe 3
    }
  }
}
