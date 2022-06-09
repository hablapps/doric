package doric
package sem

import cats.data.NonEmptyChain
import doric.SparkSessionTestWrapper
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers
import sourcecode.{FileName, Line}

import org.apache.spark.sql.types.StringType

class ErrorsSpec
    extends AnyFunSpecLike
    with SparkSessionTestWrapper
    with Matchers {

  describe("SparkErrorWrapper") {
    it("should be comparable to another SparkErrorWrapper") {
      SparkErrorWrapper(
        new Exception("this")
      ).canEqual(
        SparkErrorWrapper(
          new Exception("that")
        )
      ) shouldBe true
    }

    it("should not be comparable to another thing") {
      SparkErrorWrapper(
        new Exception("this")
      ).canEqual(
        new Exception("This is a simple exception")
      ) shouldBe false
    }

    it(
      "should generate the same hashCode as similar SparkErrorWrapper (if Analysis exception)"
    ) {
      import spark.implicits._
      val err = intercept[DoricMultiError] {
        Seq(1, 2, 3).toDF("value").select(colInt("notFound"))
      }
      val err2 = intercept[DoricMultiError] {
        Seq(4, 5, 6).toDF("value").select(colInt("notFound"))
      }

      err.errors.head.hashCode() shouldBe err2.errors.head.hashCode()
    }

    it("should generate different hashCode from another SparkErrorWrapper") {
      val err = SparkErrorWrapper(
        new Exception("this")
      )
      val err2 = SparkErrorWrapper(
        new Exception("that")
      )

      err.hashCode() shouldNot be(err2.hashCode())
    }

    it(
      "should be equals if an AnalysisException and any other exception has he same message"
    ) {
      import spark.implicits._
      val err = intercept[DoricMultiError] {
        Seq(1, 2, 3).toDF("value").select(colInt("notFound"))
      }
      val err2 = SparkErrorWrapper(
        new Exception("Cannot resolve column name \"notFound\" among (value)")
      )

      err.errors.head.equals(err2) shouldBe true
    }

    it("should NOT be equals if the second is a different DoricSingleError") {
      val err = SparkErrorWrapper(
        new Exception("this")
      )
      val err2 = ColumnTypeError("myColumn", StringType, StringType)

      err.equals(err2) shouldBe false
    }
  }

  describe("SparkMultiError") {
    it("should return unique errors") {

      val fileName1: FileName = implicitly[FileName]
      val line1: Line         = implicitly[Line]

      DoricMultiError(
        "test",
        NonEmptyChain(
          SparkErrorWrapper(new Throwable(""))(Location(fileName1, line1)),
          SparkErrorWrapper(new Throwable(""))(Location(fileName1, line1))
        )
      ).uniqueErrors.length shouldBe 1

      val fileName2: FileName = implicitly[FileName]
      val line2: Line         = implicitly[Line]

      DoricMultiError(
        "test",
        NonEmptyChain(
          SparkErrorWrapper(new Throwable(""))(Location(fileName1, line1)),
          SparkErrorWrapper(new Throwable(""))(Location(fileName2, line2))
        )
      ).uniqueErrors.length shouldBe 2
    }
  }
}
