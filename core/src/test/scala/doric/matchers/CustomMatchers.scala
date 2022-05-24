package doric.matchers

import cats.data.NonEmptyChain
import doric.sem.{DoricMultiError, DoricSingleError, Location}
import org.apache.spark.sql.AnalysisException
import org.scalatest.matchers._

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

      val order: DoricSingleError => (String, String, String) = dse =>
        (dse.getClass.getSimpleName, dse.message, dse.location.toString)

      val errors         = multiError.errors.sortBy(order)
      val expectedErrors = NonEmptyChain.fromSeq(exceptions).get.sortBy(order)

      val checkFunctions = errors.map(x => {
        x.cause match {
          case Some(_: AnalysisException) =>
            val fun: DoricSingleError => Boolean = n =>
              (
                if (x.message.last == ';')
                  x.message.substring(0, x.message.length - 1)
                else x.message
              ) == n.message
            fun
          case _ =>
            val fun: DoricSingleError => Boolean = n => x == n
            fun
        }
      })
      val res = checkFunctions
        .zipWith(expectedErrors)((f, e) => f(e))

      MatchResult(
        matches = res.reduceRight(_ && _),
        // TODO show real errors only? like, if there are two exceptions, but only one failed, show that one
        rawFailureMessage = // TODO this custom matcher shadows real assertions?
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
