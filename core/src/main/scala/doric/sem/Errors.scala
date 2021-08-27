package doric
package sem

import cats.data.NonEmptyChain

import org.apache.spark.sql.types.DataType

case class DoricMultiError(
    functionType: String,
    errors: NonEmptyChain[DoricSingleError]
) extends Throwable(errors.head.getCause) {
  override def getMessage: String = {
    val length = errors.length

    implicit class StringOps(s: String) {
      private val indentation = "  "
      def withTabs: String    = indentation + s.replaceAll("\n", s"\n$indentation")
    }

    implicit class JoinCases(errors: NonEmptyChain[DoricSingleError]) {
      def collectSide(isLeft: Boolean): Option[DoricMultiError] =
        NonEmptyChain
          .fromChain(errors.collect { case JoinDoricSingleError(x, `isLeft`) =>
            x
          })
          .map(
            DoricMultiError(
              if (isLeft) "Left dataframe" else "Right dataframe",
              _
            )
          )
    }

    def getSingularOrPlural(nErrors: Long): String =
      if (nErrors > 1) "errors" else "error"

    val leftErrors  = errors.collectSide(true).map(_.getMessage)
    val rightErrors = errors.collectSide(false).map(_.getMessage)
    val restErrors = NonEmptyChain
      .fromChain(
        errors.filter(x => !x.isInstanceOf[JoinDoricSingleError])
      )
      .map(_.map(_.getMessage).iterator.mkString("\n"))
    val stringErrors =
      List(restErrors, leftErrors, rightErrors).flatten.mkString("\n").withTabs

    s"""Found $length ${getSingularOrPlural(length)} in $functionType
       |$stringErrors
       |""".stripMargin
  }
}

sealed abstract class DoricSingleError(val cause: Option[Throwable])
    extends Throwable(cause.orNull) {
  def message: String
  def location: Location

  override def getMessage: String =
    message + s"\n\tlocated at . ${location.getLocation}"
}

case class JoinDoricSingleError(sideError: DoricSingleError, isLeft: Boolean)
    extends DoricSingleError(sideError.cause) {
  override def message: String = sideError.message

  override def location: Location = sideError.location

  val isRight: Boolean = !isLeft
}

case class ColumnTypeError(
    columnName: String,
    expectedType: DataType,
    foundType: DataType
)(implicit
    val location: Location
) extends DoricSingleError(None) {
  override def message: String =
    s"The column with name '$columnName' is of type $foundType and it was expected to be $expectedType"
}

case class ChildColumnNotFound(
    columnName: String,
    validColumns: Seq[String]
)(implicit
    val location: Location
) extends DoricSingleError(None) {
  override def message: String =
    s"No such struct field $columnName among nested columns ${validColumns
      .mkString("(", ", ", ")")}"
}

case class SparkErrorWrapper(sparkCause: Throwable)(implicit
    val location: Location
) extends DoricSingleError(Some(sparkCause)) {
  override def message: String = sparkCause.getMessage
}

object Location {
  implicit def location(implicit
      line: sourcecode.Line,
      file: sourcecode.FileName
  ): Location =
    Location(file.value, line.value)
}

case class Location(
    fileName: sourcecode.FileName,
    lineNumber: sourcecode.Line
) {
  def getLocation: String = s"(${fileName.value}:${lineNumber.value})"
}
