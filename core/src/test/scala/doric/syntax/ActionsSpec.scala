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
            colLong("id").action.validate.all(
              _ <= 10L,
              _ > 0L
            ) === col("id")
          )
          .as[Boolean]
          .collect()
          .reduce(_ && _) shouldBe true
      }
      it("should return error if the validation is not passed") {
        import doric.implicitConversions.literalConversion
        df.testErrorColumn(
          colLong("id").action.validate
            .all(
              _ > 100L,
              _ > 1L
            ),
          "Validation error: Not all rows passed the validation (id > 100) (0 of 3 were valid)",
          "Validation error: Not all rows passed the validation (id > 1) (2 of 3 were valid)"
        )
      }
      it("should validate if any is null") {
        val noneNullCol = colLong("id").action.validate.noneNull
        noneNullCol.elem
          .run(df)
          .toEither
          .value
        List(Some(1L), None)
          .toDF("id")
          .testErrorColumn(
            noneNullCol,
            "Validation error: Not all rows passed the validation (id IS NOT NULL) (1 of 2 were valid)"
          )
      }
    }
  }

}
