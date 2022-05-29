package doric.matchers

import doric.sem.{DoricMultiError, DoricSingleError}
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
  private type DSE = DoricSingleError

  private def msgErrors(
      dme: Set[DSE],
      expectedDme: Set[DSE]
  ): Either[String, Option[String]] = {

    if (dme.size == expectedDme.size) {
      val errors: Set[Either[(DSE, DSE), DSE]] = dme
        .zip(expectedDme)
        .map { case (f, e) =>
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
    * DoricMultiError custom matcher.
    * This tests if `multiError` & `exceptions` elements are __ALL the same__
    *
    * @param exceptions Non empty seq of DoricSingleError to compare with the actual errors (`multiError`)
    */
  class DoricErrorMatcherAll(exceptions: Seq[DSE])
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

      val msgNot =
        s"‼️ Expected NOT to find the following errors:\n${expectedErrors.map(_.toString + "\n")}\n"

      MatchResult(errors == expectedErrors, msg, msgNot)
    }
  }

  /**
    * DoricMultiError custom matcher.
    * This tests if __ALL__ `exceptions` are __included__ at `multiError`
    *
    * @param exceptions Non empty seq of DoricSingleError to compare with the actual errors (`multiError`)
    */
  class DoricErrorMatcher(exceptions: Seq[DSE])
      extends Matcher[DoricMultiError] {
    override def apply(multiError: DoricMultiError): MatchResult = {

      val errors         = multiError.errors.toChain.toList.toSet
      val expectedErrors = exceptions.toSet

      val found    = errors.intersect(expectedErrors)
      val notFound = expectedErrors.diff(found)

      val msg =
        s"‼️ Expected to find the following errors:\n${expectedErrors.map(_.toString + "\n")}\n" +
          s"among the actual errors:\n${errors.map(_.toString + "\n")}\n" +
          "============================\n" +
          s"✅ Expected & found       ==> \n${found.map(_.toString + "\n")}\n" +
          s"‼️ Expected but not found ==> \n${notFound.map(_.toString + "\n")}\n"

      val msgNot =
        s"‼️ Expected NOT to find the following errors:\n${expectedErrors.map(_.toString + "\n")}\n" +
          s"among the actual errors:\n${errors.map(_.toString + "\n")}\n"

      MatchResult(found.size == expectedErrors.size, msg, msgNot)
    }
  }

  def containAllErrors(exception: DSE, exceptions: DSE*) =
    new DoricErrorMatcherAll(exception +: exceptions)

  def containErrors(exception: DSE, exceptions: DSE*) =
    new DoricErrorMatcher(exception +: exceptions)
}
