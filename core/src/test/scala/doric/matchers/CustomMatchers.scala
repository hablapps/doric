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
  private def msgErrors(
      dme: Set[DoricSingleError],
      expectedDme: Set[DoricSingleError]
  ): Either[String, Option[String]] = {

    if (dme.size == expectedDme.size) {
      val errors: Set[
        Either[(DoricSingleError, DoricSingleError), DoricSingleError]
      ] = dme.zip(expectedDme).map { case (f, e) =>
        if (f == e) Right(e)
        else Left(e, f)
      }

      if (errors.exists(_.isLeft)) {
        val fmtErrors = errors.map {
          case Left((e, f)) =>
            val tuples = f.cause match {
              case Some(c: AnalysisException) =>
                (e.toString, s"${f.getClass.getName}(${c.message})")
              case _ => (e.toString, f.toString)
            }
            s"‼️ ==> Expected ${tuples._1}\nbut found\n${tuples._2}"
          case Right(x) =>
            s"✅ ==> Expected and found ${x.getClass.getName}(${x.message})\n"
        }
        Right(Some(fmtErrors.mkString("\n======\n")))
      } else
        Right(None)
    } else
      Left(
        s"‼️ Expected\n${expectedDme.map(_.toString + "\n")}\n" +
          s"but found\n${dme.map(_.toString + "\n")}"
      )
  }

  /**
    * DoricSingleError custom matcher
    * @param exceptions TODO
    */
  class DoricErrorMatcher(exceptions: Seq[DoricSingleError])
      extends Matcher[DoricMultiError] {
    override def apply(multiError: DoricMultiError): MatchResult = {

      val errors         = multiError.errors.toChain.toList.toSet
      val expectedErrors = exceptions.toSet
      val msg = msgErrors(errors, expectedErrors) match {
        case Right(Some(x)) => x
        case Left(x)        => x
        case _ =>
          "If you see this message, something went south when comparing doric errors..." +
            " Please check/open an issue to review it: https://github.com/hablapps/doric/issues"
      }

      MatchResult(
        matches = errors == expectedErrors,
        rawFailureMessage = msg,
        rawNegatedFailureMessage =
          s"Doric errors expected and found:\n${multiError.errors}" // TODO
      )
    }
  }

  def includeErrors(
      exception: DoricSingleError,
      exceptions: DoricSingleError*
  ) = new DoricErrorMatcher(exception +: exceptions)
}
