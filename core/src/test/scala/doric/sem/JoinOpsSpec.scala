package doric
package sem

import doric.implicitConversions._
import org.apache.spark.sql.types.{LongType, StringType}

class JoinOpsSpec extends DoricTestElements {

  private val id          = "id"
  private val otherColumn = "otherColumn"
  private val left = spark
    .range(10)
    .toDF()
    .withColumn(
      otherColumn,
      concat(colLong(id).cast[String], "left")
    )
    .toDF()
    .filter(colLong(id) > 3L)
  private val right = spark
    .range(10)
    .toDF()
    .withColumn(
      otherColumn,
      concat(colLong(id).cast[String], "right")
    )
    .filter(colLong(id) < 7L)

  describe("join ops") {

    it("works with join of same name and type columns") {
      val col1 = colLong(id).cast[String]
      val badRight = right
        .withColumn(id, col1)

      left.join(right, "left", colLong(id))
      left.join(right, "right", colLong(id))
      left.join(right, "inner", colLong(id))
      left.join(right, "outer", colLong(id))

      intercept[DoricMultiError] {
        val value1 = colLong(id)
        left.join(badRight, "left", value1)
      } should includeErrors(
        JoinDoricSingleError(
          ColumnTypeError("id", LongType, StringType),
          isLeft = false
        )
      )
    }

    it("should join typesafety") {

      val joinFunction: DoricJoinColumn =
        LeftDF.colLong(id) === RightDF.colLong(id)

      left.join(right, "inner", joinFunction)

      val badJoinFunction: DoricJoinColumn =
        LeftDF.colString(id) ===
          RightDF.colString(id + "entifier")

      intercept[DoricMultiError] {
        left.join(right, "inner", badJoinFunction)
      } should includeErrors(
        JoinDoricSingleError(
          ColumnTypeError("id", StringType, LongType),
          isLeft = true
        ),
        JoinDoricSingleError(
          SparkErrorWrapper(
            new Exception(
              "Cannot resolve column name \"" + id + "entifier\" among (" + id + ", " + otherColumn + ")"
            )
          ),
          isLeft = false
        )
      )

      val joinFunction2: DoricJoinColumn =
        LeftDF(colLong(id)) === RightDF.colLong(id)

      left.join(right, "inner", joinFunction2)

      val badJoinFunction2: DoricJoinColumn =
        LeftDF(colString(id)) ===
          RightDF(colString(id + "entifier"))

      intercept[DoricMultiError] {
        left.join(right, "inner", badJoinFunction2)
      } should includeErrors(
        JoinDoricSingleError(
          ColumnTypeError("id", StringType, LongType),
          isLeft = true
        ),
        JoinDoricSingleError(
          SparkErrorWrapper(
            new Exception(
              "Cannot resolve column name \"" + id + "entifier\" among (" + id + ", " + otherColumn + ")"
            )
          ),
          isLeft = false
        )
      )
    }

    it("should prevent key ambiguity with innerJoinDropRightKey") {
      val resultDF = left.innerJoinKeepLeftKeys(right, colLong(id))

      val keyAsString = "keyAsString"
      val col2        = colLong(id).cast[String]
      resultDF.withColumn(keyAsString, col2)
      resultDF.schema.length shouldBe 3
    }

    it("should prevent non key ambiguity using colFromDf") {
      val resultDF = left.innerJoinKeepLeftKeys(right, colLong(id))

      val nonKeyColRight = "nonKeyColRight"
      val nonKeyColLeft  = "nonKeyColLeft"
      val value1         = colFromDF[String](otherColumn, right)
      val value2         = colFromDF[String](otherColumn, left)
      resultDF
        .withColumn(nonKeyColRight, value1)
        .withColumn(nonKeyColLeft, value2)
        .collectCols(
          (colString(
            nonKeyColRight
          ) === value1) && (colString(
            nonKeyColLeft
          ) === value2)
        )
        .forall(identity) shouldBe true
      resultDF.schema.length shouldBe 3
    }
  }
}
