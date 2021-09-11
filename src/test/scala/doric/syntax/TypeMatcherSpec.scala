package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

class TypeMatcherSpec
    extends DoricTestElements
    with TypeMatcher
    with ArrayColumns
    with EitherValues
    with Matchers {

  import spark.implicits._

  describe("Any column Ops") {
    describe("matchTo") {
      val df = List((List(1, 2, 3), 1)).toDF("colArr", "int")
      it("should check the first valid match") {
        val testColumn = matchToType[Int]("colArr")
          .caseType[Int](identity)
          .caseType[String](_.unsafeCast)
          .caseType[Array[Int]](_.getIndex(0) + col("int"))
          .inOtherCase(12.lit)

        df.withColumn("result", testColumn)
          .select("result")
          .as[Int]
          .head() shouldBe 2
      }

      it("should return the default parameter") {
        val testColumn = matchToType[Int]("colArr")
          .caseType[Int](identity)
          .caseType[String](_.unsafeCast)
          .caseType[Array[String]](_.getIndex(0).unsafeCast)
          .inOtherCase(12.lit)

        df.withColumn("result", testColumn)
          .select("result")
          .as[Int]
          .head() shouldBe 12
      }

      it("should return an error in case of valid match has an error") {
        val testColumn = matchToType[Int]("colArr")
          .caseType[Int](identity)
          .caseType[String](_.unsafeCast)
          .caseType[Array[Int]](_.getIndex(0) + col("int2"))
          .inOtherCase(12.lit)

        val errors = testColumn.elem.run(df).toEither.left.value
        errors.length shouldBe 1
        errors.head.message shouldBe "Cannot resolve column name \"int2\" among (colArr, int)"
      }

      it(
        "should return an error if no mach used and the default case has an error"
      ) {
        val testColumn = matchToType[Int]("colArr")
          .caseType[Int](identity)
          .caseType[String](_.unsafeCast)
          .inOtherCase(col("int3"))

        val errors = testColumn.elem.run(df).toEither.left.value
        errors.length shouldBe 1
        errors.head.message shouldBe "Cannot resolve column name \"int3\" among (colArr, int)"
      }

      it(
        "should return an error if no mach used and no default case added"
      ) {
        val testColumn = matchToType[Int]("colArr")
          .caseType[Int](identity)
          .caseType[String](_.unsafeCast)
          .inOtherCaseError

        val errors = testColumn.elem.run(df).toEither.left.value
        errors.length shouldBe 1
        errors.head.message shouldBe "The matched column with name 'colArr' is of type IntegerType and it was expected to be one of [StringType, IntegerType]"
      }
    }
  }
}
