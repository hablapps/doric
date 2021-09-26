package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

class ActionsSpec
    extends DoricTestElements
    with Actions
    with EitherValues
    with Matchers {

  import spark.implicits._

  describe("actions") {
    describe("validations") {
      val df = List(1L, 2L, 3L).toDF("id")
      it("should pass the same column if the validation is passed") {
        import doric.implicitConversions.literalConversion
        df
          .select(
            colLong("id").action.validation.all(_ <= 10L) === col("id")
          )
          .as[Boolean]
          .collect()
          .reduce(_ && _) shouldBe true
      }
      it("should return error if the validation is not passed") {
        import doric.implicitConversions.literalConversion
        df.testErrorColumn(
          colLong("id").action.validation
            .all(_ > 100L),
          "Validation error: Not all rows passed the validation (id > 100) (0 of 3 were valid)"
        )
      }
      it("should validate if any is null") {
        val noneNullCol = colLong("id").action.validation.noneNull
        noneNullCol.elem
          .run(df)
          .toEither
          .value
        List(Some(1L), None).toDF("id").testErrorColumn(
          noneNullCol,
          "Validation error: Not all rows passed the validation (id IS NOT NULL) (1 of 2 were valid)"
        )
      }
    }
  }

}
