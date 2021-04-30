package habla.doric

import cats.data.NonEmptyChain

import org.apache.spark.sql.types.DataType

case class DoricMultiError(errors: NonEmptyChain[Throwable])
    extends Throwable(errors.head.getCause) {
  override def getMessage: String =
    (s"found ${errors.length} errors" +: errors.map(_.getMessage)).iterator
      .mkString("\n")
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
