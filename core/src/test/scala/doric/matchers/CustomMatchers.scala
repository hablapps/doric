package doric.matchers

import cats.data.NonEmptyChain
import doric.sem.{DoricMultiError, DoricSingleError}
import org.scalatest._
import matchers._

/**
  * Object to ease the import
  */
object CustomMatchers extends CustomMatchers

/**
  * Custom Doric errors matcher
  */
trait CustomMatchers {
  class DoricErrorMatcher(exceptions: DoricSingleError*)
      extends Matcher[DoricMultiError] {
    override def apply(multiError: DoricMultiError): MatchResult = {

      val order: DoricSingleError => (String, String) = dse =>
        (dse.getClass.getSimpleName, dse.getMessage)

      val errors         = multiError.errors.sortBy(order)
      val expectedErrors = NonEmptyChain.fromSeq(exceptions).get.sortBy(order)

      MatchResult(
        matches = errors == expectedErrors,
        rawFailureMessage =
          s"Expected\n${NonEmptyChain(exceptions)}\nbut found\n${multiError.errors}",
        rawNegatedFailureMessage =
          s"Doric errors expected and found:\n${multiError.errors}"
      )
    }
  }

  def includeErrors(
      exception: DoricSingleError,
      exceptions: DoricSingleError*
  ) = new DoricErrorMatcher(exception +: exceptions: _*)
}
