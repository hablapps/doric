package doric
package sem

import cats.data.{NonEmptyChain, NonEmptySet}
import cats.implicits._
import cats.Order

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.DataType

case class DoricMultiError(
    functionType: String,
    errors: NonEmptyChain[DoricSingleError]
) extends Throwable(errors.head.getCause) {

  lazy val uniqueErrors: NonEmptySet[DoricSingleError] = {
    val list1: List[DoricSingleError] = errors.toNonEmptyList.toList
    NonEmptySet.fromSet(scala.collection.immutable.SortedSet(list1: _*)).get
  }

  override def getMessage: String = {
    val length = uniqueErrors.length

    implicit class StringOps(s: String) {
      private val indentation = "  "
      def withTabs: String = indentation + s.replaceAll("\n", s"\n$indentation")
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

    def getSingularOrPlural(nErrors: Int): String =
      if (nErrors > 1) "errors" else "error"

    val leftErrors  = errors.collectSide(true).map(_.getMessage)
    val rightErrors = errors.collectSide(false).map(_.getMessage)
    val restErrors = NonEmptySet
      .fromSet(
        uniqueErrors.filter(x => !x.isInstanceOf[JoinDoricSingleError])
      )
      .map(_.map(_.getMessage).toNonEmptyList.sorted.toList.mkString("\n"))
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

object DoricSingleError {

  implicit val order: Order[DoricSingleError] = Order.by(x =>
    (
      x.location.fileName.value,
      x.location.lineNumber.value,
      x.message,
      x match {
        case x: JoinDoricSingleError if x.isLeft => 1
        case _: JoinDoricSingleError             => 2
        case _                                   => 0
      }
    )
  )

  implicit val ordering: Ordering[DoricSingleError] = order.toOrdering
}

case class JoinDoricSingleError(sideError: DoricSingleError, isLeft: Boolean)
    extends DoricSingleError(sideError.cause) {
  override def message: String = sideError.message

  override def location: Location = sideError.location

  val isRight: Boolean = !isLeft
}

case class ColumnMultiTypeError(
    columnName: String,
    expectedTypes: List[DataType],
    foundType: DataType
)(implicit
    val location: Location
) extends DoricSingleError(None) {

  override def message: String =
    s"The matched column with name '$columnName' was expected to be one of ${expectedTypes
        .mkString("[", ", ", "]")} but is of type $foundType"
}

case class ColumnTypeError(
    columnName: String,
    expectedType: DataType,
    foundType: DataType
)(implicit
    val location: Location
) extends DoricSingleError(None) {

  override def message: String =
    s"The column with name '$columnName' was expected to be $expectedType but is of type $foundType"
}

case class SparkErrorWrapper(sparkCause: Throwable)(implicit
    val location: Location
) extends DoricSingleError(Some(sparkCause)) {
  override def message: String = sparkCause.getMessage

  /**
    * This will get a "common" message across all spark versions.
    */
  private lazy val eqSpark: String => String = {
    case str
        if str.startsWith("Cannot resolve column name")
          && (str.last == ';') // Not present since spark 3.0.0
        =>
      str.substring(0, str.length - 1)
    case str
        if str.startsWith("cannot resolve '")
          && str.contains("`") // Not present since spark 3.2.1
          && str.contains("given input columns") =>
      str.replace("`", "")
    case str => str
  }

  override def canEqual(that: Any): Boolean = that match {
    case _: SparkErrorWrapper => true
    case _                    => false
  }

  override def equals(obj: Any): Boolean = (sparkCause, obj) match {
    case (ae: AnalysisException, SparkErrorWrapper(exc)) =>
      // As we cannot create an Analysis exception, we only compare messages
      eqSpark(ae.message) == eqSpark(exc.getMessage)
    case (ae, SparkErrorWrapper(exc)) => ae == exc
    case _                            => false
  }

  override def hashCode(): Int = sparkCause match {
    case a: AnalysisException => eqSpark(a.message).##
    case _                    => super.hashCode()
  }
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
