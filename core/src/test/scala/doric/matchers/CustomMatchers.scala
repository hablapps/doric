package doric.matchers

import doric.sem.{DoricMultiError, DoricSingleError}
import org.scalatest.matchers._

/**
  * Object to ease the import
  */
object CustomMatchers extends CustomMatchers

/**
  * Custom Doric errors matcher
  */
trait CustomMatchers {
  private type DSE = DoricSingleError

  private def compare(
      err: DoricMultiError,
      exp: Seq[DSE]
  ): (Set[DSE], Set[DSE], Set[DSE], Set[DSE], Set[DSE]) = {
    val errors         = err.uniqueErrors.toSortedSet
    val expectedErrors = exp.toSet

    val found          = errors.intersect(expectedErrors)
    val notFound       = expectedErrors.diff(found)
    val notExpButFound = errors.diff(found)

    (errors, expectedErrors, found, notFound, notExpButFound)
  }

  private def getIfNotEmpty(s: Set[DSE], str: String): String =
    if (s.isEmpty) ""
    else s"‼️ $str ==> \n${s.map(_.toString + "\n").mkString("\n")}\n"

  class DoricErrorMatcherAll(exceptions: Seq[DSE])
      extends Matcher[DoricMultiError] {
    override def apply(multiError: DoricMultiError): MatchResult = {

      val (errors, expectedErrors, found, notFound, notExpButFound) =
        compare(multiError, exceptions)

      val msg = "Expected to match exactly\n" +
        getIfNotEmpty(notFound, "Expected but not found") +
        getIfNotEmpty(notExpButFound, "Not expected but found")

      val msgNot = getIfNotEmpty(
        expectedErrors,
        "Expected NOT to match the following errors"
      )

      MatchResult(errors.size == found.size && notFound.isEmpty, msg, msgNot)
    }
  }

  class DoricErrorMatcher(exceptions: Seq[DSE])
      extends Matcher[DoricMultiError] {
    override def apply(multiError: DoricMultiError): MatchResult = {

      val (_, expectedErrors, found, notFound, _) =
        compare(multiError, exceptions)

      val msg = "Expected to contain errors\n" +
        getIfNotEmpty(notFound, "but not found")

      val msgNot = getIfNotEmpty(
        expectedErrors,
        "Expected NOT to contain the following errors"
      )

      MatchResult(found.size == expectedErrors.size, msg, msgNot)
    }
  }

  /**
    * DoricMultiError custom matcher.
    * This tests if `multiError` & `exceptions` elements are __ALL the same__
    *
    * @param exceptions Non empty seq of DoricSingleError to compare with the actual errors (`multiError`)
    */
  def containAllErrors(exception: DSE, exceptions: DSE*) =
    new DoricErrorMatcherAll(exception +: exceptions)

  /**
    * DoricMultiError custom matcher.
    * This tests if __ALL__ `exceptions` are __included__ at `multiError`
    *
    * @param exceptions Non empty seq of DoricSingleError to compare with the actual errors (`multiError`)
    */

  def containErrors(exception: DSE, exceptions: DSE*) =
    new DoricErrorMatcher(exception +: exceptions)
}
