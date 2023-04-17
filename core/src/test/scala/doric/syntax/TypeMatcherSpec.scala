package doric
package syntax

import doric.sem.{ColumnMultiTypeError, ColumnNotFound, DoricMultiError}

import org.apache.spark.sql.types.{IntegerType, StringType}

class TypeMatcherSpec
    extends DoricTestElements
    with TypeMatcher
    with ArrayColumns {

  import doric.implicitConversions.stringCname
  import spark.implicits._

  describe("Any column Ops") {
    describe("matchTo") {
      val df     = List((List(1, 2, 3), 1, "1200")).toDF("colArr", "int", "str")
      val result = "result"
      it("should check the first valid match") {
        val testColumn: String => IntegerColumn = matchToType[Int](_)
          .caseType[Int](identity)
          .caseType[String](_.unsafeCast)
          .caseType[Array[Int]](_.getIndex(0) + col("int"))
          .inOtherCase(12.lit)

        df.withColumn(result, testColumn("colArr"))
          .selectCName(result)
          .as[Int]
          .head() shouldBe 2

        df.withColumn(result, testColumn("int"))
          .selectCName(result)
          .as[Int]
          .head() shouldBe 1

        df.withColumn(result, testColumn("str"))
          .selectCName(result)
          .as[Int]
          .head() shouldBe 1200
      }

      it("should return the default parameter") {
        val testColumn = matchToType[Int]("colArr")
          .caseType[Int](identity)
          .caseType[String](_.unsafeCast)
          .caseType[Array[String]](_.getIndex(0).unsafeCast)
          .inOtherCase(12.lit)

        df.withColumn(result, testColumn)
          .selectCName(result)
          .as[Int]
          .head() shouldBe 12
      }

      it("should return an error in case of valid match has an error") {
        val testColumn = matchToType[Int]("colArr")
          .caseType[Int](identity)
          .caseType[String](_.unsafeCast)
          .caseType[Array[Int]](_.getIndex(0) + col("int2"))
          .inOtherCase(12.lit)

        intercept[DoricMultiError] {
          df.select(testColumn)
        } should containAllErrors(
          ColumnNotFound("int2", List("colArr", "int", "str"))
        )
      }

      it(
        "should return an error if no mach used and the default case has an error"
      ) {
        val testColumn = matchToType[Int]("colArr")
          .caseType[Int](identity)
          .caseType[String](_.unsafeCast)
          .inOtherCase(col("int3"))

        intercept[DoricMultiError] {
          df.select(testColumn)
        } should containAllErrors(
          ColumnNotFound("int3", List("colArr", "int", "str"))
        )
      }

      it("should return an error if no mach used and no default case added") {
        val testColumn = matchToType[Int]("colArr")
          .caseType[Int](identity)
          .caseType[String](_.unsafeCast)
          .inOtherCaseError

        intercept[DoricMultiError] {
          df.select(testColumn)
        } should containAllErrors(
          ColumnMultiTypeError(
            "colArr",
            List(StringType, IntegerType),
            IntegerType
          )
        )
      }
    }
  }
}
