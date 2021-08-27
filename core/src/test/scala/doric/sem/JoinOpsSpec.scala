package doric
package sem

import doric.implicitConversions._
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues

class JoinOpsSpec extends DoricTestElements with Matchers with EitherValues {

  import spark.implicits._

  private val left = spark
    .range(10)
    .toDF()
    .withColumn(
      "otherColumn",
      concat(colLong("id").cast[String], "left")
    )
    .toDF()
    .filter(colLong("id") > 3L)
  private val right = spark
    .range(10)
    .toDF()
    .withColumn(
      "otherColumn",
      concat(colLong("id").cast[String], "right")
    )
    .filter(colLong("id") < 7L)

  describe("join ops") {

    it("works with join of same name and type columns") {
      val badRight = right
        .withColumn("id", colLong("id").cast[String])

      left.join(right, "left", colLong("id"))
      left.join(right, "right", colLong("id"))
      left.join(right, "inner", colLong("id"))
      left.join(right, "outer", colLong("id"))

      val errors = intercept[DoricMultiError] {
        val value1 = colLong("id")
        left.join(badRight, "left", value1)
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
