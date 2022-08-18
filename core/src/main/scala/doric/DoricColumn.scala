package doric

import cats.data.{Kleisli, NonEmptyChain, Validated}
import cats.implicits._
import doric.sem.{ColumnTypeError, DoricSingleError, Location, SparkErrorWrapper}
import doric.syntax.ColGetters
import doric.types.{LiteralSparkType, SparkType}

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.types.DataType

sealed trait DoricColumn[T] {
  val elem: Doric[Column]
}

case class NamedDoricColumn[T] private[doric] (
    override val elem: Doric[Column],
    columnName: String
) extends DoricColumn[T]

object NamedDoricColumn {
  def apply[T](
      column: DoricColumn[T],
      columnName: String
  ): NamedDoricColumn[T] =
    NamedDoricColumn[T](column.elem.map(_.as(columnName)), columnName)
}

case class TransformationDoricColumn[T] private[doric] (
    override val elem: Doric[Column]
) extends DoricColumn[T]

case class LiteralDoricColumn[T] private[doric] (
    override val elem: Doric[Column],
    columnValue: T
) extends DoricColumn[T]

object LiteralDoricColumn {
  def apply[T: SparkType: LiteralSparkType](
      value: T
  )(implicit l: Location): LiteralDoricColumn[T] =
    LiteralDoricColumn(
      Kleisli { _ => LiteralSparkType[T].literal(value) },
      value
    )

  implicit class LiteralGetter[T](litCol: LiteralDoricColumn[T]) {
    def getColumnValueAsSparkValue(implicit
        c: LiteralSparkType[T]
    ): c.OriginalSparkType =
      c.literalTo(litCol.columnValue)
  }
}

object DoricColumn extends ColGetters[NamedDoricColumn] {

  private[doric] def apply[T](dcolumn: Doric[Column]): DoricColumn[T] =
    TransformationDoricColumn(dcolumn)

  private[doric] def apply[T](f: Dataset[_] => Column): DoricColumn[T] =
    Kleisli[DoricValidated, Dataset[_], Column](f(_).valid).toDC

  override protected def constructSide[T](
      column: Doric[Column],
      colName: String
  ): NamedDoricColumn[T] = NamedDoricColumn(column, colName)

  def apply[T: SparkType](
      column: Column
  )(implicit location: Location): DoricColumn[T] = {
    column.expr match {
      case UnresolvedAttribute(nameParts) =>
        col(
          nameParts.map(x => if (x.contains(".")) s"`$x`" else x).mkString(".")
        )
      case _ =>
        Kleisli[DoricValidated, Dataset[_], Column](df => {
          try {
            val dataType: DataType =
              try {
                column.expr.dataType
              } catch {
                case _: Throwable => df.select(column).schema.head.dataType
              }
            if (SparkType[T].isEqual(dataType))
              Validated.valid(column)
            else
              ColumnTypeError(
                df.select(column).schema.head.name,
                SparkType[T].dataType,
                dataType
              ).invalidNec
          } catch {
            case e: Throwable => SparkErrorWrapper(e).invalidNec
          }
        }).toDC
    }
  }

  private[doric] def uncheckedType(column: String): DoricColumn[_] = {
    Kleisli[DoricValidated, Dataset[_], Column](df => {
      try {
        Validated.valid(df(column))
      } catch {
        case e: Throwable => SparkErrorWrapper(e).invalidNec
      }
    }).toDC
  }

  private[doric] def apply[T](
      errors: NonEmptyChain[DoricSingleError]
  ): DoricColumn[T] = {
    Kleisli[DoricValidated, Dataset[_], Column]((_: Dataset[_]) =>
      errors.invalid
    )
  }.toDC

  private[doric] def apply[T](
      error: DoricSingleError
  ): DoricColumn[T] = {
    Kleisli[DoricValidated, Dataset[_], Column]((_: Dataset[_]) =>
      error.invalidNec
    )
  }.toDC

  @inline private[doric] def sparkFunction[T, O](
      column: DoricColumn[T],
      other: DoricColumn[T],
      f: (Column, Column) => Column
  ): DoricColumn[O] =
    (column.elem, other.elem).mapN(f).toDC
}
