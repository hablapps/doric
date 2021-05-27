package habla.doric

import cats.data.NonEmptyChain

import org.apache.spark.sql.types.DataType

object ErrorUtils {
  def getSingularOrPlural(nErrors: Long): String =
    if (nErrors > 1) "errors" else "error"
}

case class DoricJoinMultiError(
    errors: NonEmptyChain[JoinDoricSingleError]
) extends Throwable(
      errors.head.getCause
    ) {
  override def getMessage: String = {
    implicit class StringOps(s: String) {
      def addTabs: String = s.replaceAll("\n", "\n\t")
    }

    implicit class DoricMulti(multiError: DoricMultiError) {
      def getForSide(side: String): String =
        s"$side dataframe:\n${multiError.getMessage.addTabs}"
    }

    val numErrors: Long = errors.length
    val leftErrors = NonEmptyChain
      .fromChain(errors.toChain.filter(_.isLeft))
      .map(DoricMultiError(_).getForSide("Left"))
    val rightErrors = NonEmptyChain
      .fromChain(errors.toChain.filter(_.isRight))
      .map(DoricMultiError(_).getForSide("Right"))

    val sidesErrors = (leftErrors, rightErrors) match {
      case (Some(l), Some(r)) => l + "\n" + r
      case (Some(l), _) => l
      case (_, Some(r)) => r
      case _ => "impossible!!!"
    }

    s"found $numErrors ${ErrorUtils.getSingularOrPlural(numErrors)} in join:\n$sidesErrors"
  }
}

case class DoricMultiError(errors: NonEmptyChain[DoricSingleError])
    extends Throwable(errors.head.getCause) {
  override def getMessage: String = {
    val length = errors.length
    (s"found $length ${ErrorUtils.getSingularOrPlural(length)}" +: errors.map(_.getMessage)).iterator
      .mkString("\n")
  }
}

sealed abstract class JoinDoricSingleError(sideCause: DoricSingleError)
    extends DoricSingleError(sideCause.getCause) {
  override def message: String = sideCause.message

  override def location: Location = sideCause.location

  def isLeft: Boolean
  def isRight: Boolean = !isLeft
}
case class LeftDfError(sideCause: DoricSingleError)
    extends JoinDoricSingleError(sideCause) {
  override def isLeft: Boolean = true
}
case class RightDfError(sideCause: DoricSingleError)
    extends JoinDoricSingleError(sideCause) {
  override def isLeft: Boolean = false
}

abstract class DoricSingleError(cause: Throwable) extends Throwable(cause) {
  def message: String
  def location: Location

  override def getMessage: String =
    message + s"\n\tlocated at . ${location.getLocation}"
}

case class ColumnTypeError(
    columnName: String,
    expectedType: DataType,
    foundType: DataType,
    cause: Throwable = null
)(implicit
    val location: Location
) extends DoricSingleError(cause) {
  override def message: String =
    s"The column with name '$columnName' is of type $foundType and it was expected to be $expectedType"
}

case class ChildColumnNotFound(
    columnName: String,
    validColumns: Seq[String],
    cause: Throwable = null
)(implicit
    val location: Location
) extends DoricSingleError(cause) {
  override def message: String =
    s"No such struct field $columnName among nested columns ${validColumns
      .mkString("(", ", ", ")")}"
}

case class SparkErrorWrapper(cause: Throwable)(implicit
    val location: Location
) extends DoricSingleError(cause) {
  override def message: String = cause.getMessage
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
