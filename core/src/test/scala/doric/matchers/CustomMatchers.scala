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
  class DoricErrorMatcher(exception: DoricSingleError*)
      extends Matcher[DoricMultiError] {
    override def apply(multiError: DoricMultiError): MatchResult = MatchResult(
      matches = multiError.errors == NonEmptyChain(exception),
      rawFailureMessage =
        s"Expected\n${NonEmptyChain(exception)}\nbut found\n${multiError.errors}",
      rawNegatedFailureMessage = s"Doric errors expected and found:\n${multiError.errors}"
    )
  }

  def includeErrors(exception: DoricSingleError*) = new DoricErrorMatcher(
    exception: _*
  )
}
