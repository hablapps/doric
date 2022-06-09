package doric.matchers

import cats.data.NonEmptyChain
import doric.DoricTestElements
import doric.sem.{ColumnTypeError, DoricMultiError, DoricSingleError, SparkErrorWrapper}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.exceptions.TestFailedException

class CustomMatchersSpec extends DoricTestElements {

  private def throwErrors(
      dse: DoricSingleError,
      dseList: DoricSingleError*
  ): Nothing = {
    throw DoricMultiError(
      "",
      NonEmptyChain.fromSeq((dse +: dseList).toList).get
    )
  }

  private lazy val sparkErrorWrapper = SparkErrorWrapper(
    new Exception(
      "Cannot resolve column name \"something2\" among (col, something)"
    )
  )

  private lazy val columnTypeError =
    ColumnTypeError("something", IntegerType, StringType)

  describe("CustomMatchers for doric errors (containAllErrors)") {

    it("should check if one error is the same as expected") {
      intercept[DoricMultiError](
        throwErrors(sparkErrorWrapper)
      ) should containAllErrors(
        sparkErrorWrapper
      )
    }

    it("should check if one error is NOT the same as expected") {
      intercept[DoricMultiError](
        throwErrors(sparkErrorWrapper)
      ) shouldNot containAllErrors(
        columnTypeError
      )
    }

    it("should fail if expected errors < than actual errors") {
      intercept[TestFailedException] {
        intercept[DoricMultiError](
          throwErrors(sparkErrorWrapper, columnTypeError)
        ) should containAllErrors(
          columnTypeError
        )
      }
    }

    it("should fail if expected errors > than actual errors") {
      intercept[TestFailedException] {
        intercept[DoricMultiError](
          throwErrors(sparkErrorWrapper, sparkErrorWrapper)
        ) should containAllErrors(
          columnTypeError,
          sparkErrorWrapper
        )
      }
    }

    it(
      "should check if all errors should NOT be the same as expected but they are (unordered)"
    ) {
      intercept[TestFailedException] {
        intercept[DoricMultiError](
          throwErrors(sparkErrorWrapper, columnTypeError)
        ) shouldNot containAllErrors(
          columnTypeError,
          sparkErrorWrapper
        )
      }
    }
  }

  describe("CustomMatchers for doric errors (containErrors)") {

    it("should check if one error is the same as expected") {
      intercept[DoricMultiError](
        throwErrors(sparkErrorWrapper)
      ) should containErrors(
        sparkErrorWrapper
      )
    }

    it("should check if one error is NOT the same as expected") {
      intercept[DoricMultiError](
        throwErrors(sparkErrorWrapper)
      ) shouldNot containErrors(
        columnTypeError
      )
    }

    it("should test if actual multiple error contains one expected error") {
      intercept[DoricMultiError](
        throwErrors(sparkErrorWrapper, columnTypeError)
      ) should containErrors(
        columnTypeError
      )
    }

    it(
      "should test if actual multiple error contains multiple expected errors (unordered)"
    ) {
      intercept[DoricMultiError](
        throwErrors(sparkErrorWrapper, columnTypeError)
      ) should containErrors(
        columnTypeError,
        sparkErrorWrapper
      )
    }

    it(
      "should check if all errors should NOT be the same as expected but they are (unordered)"
    ) {
      intercept[TestFailedException] {
        intercept[DoricMultiError](
          throwErrors(sparkErrorWrapper, columnTypeError)
        ) shouldNot containErrors(
          columnTypeError,
          sparkErrorWrapper
        )
      }
    }
  }
}
