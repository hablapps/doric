package doric

import cats.data.{Kleisli, NonEmptyChain, Validated}
import cats.implicits.{catsSyntaxTuple2Semigroupal, catsSyntaxValidatedId, catsSyntaxValidatedIdBinCompat0}
import doric.sem.{ColumnTypeError, DoricSingleError, Location, SparkErrorWrapper}
import doric.syntax.ColGetters
import doric.types.SparkType

import org.apache.spark.sql.{Column, Dataset}

sealed trait DoricColumn[T] {
  val elem: Doric[Column]
}

case class NamedDoricColumn[T] private[doric] (
    override val elem: Doric[Column],
    name: CName
) extends DoricColumn[T]

case class TransformationDoricColumn[T] private[doric] (
    override val elem: Doric[Column]
) extends DoricColumn[T]

object NamedDoricColumn {
  def apply[T](column: DoricColumn[T], name: CName): NamedDoricColumn[T] =
    NamedDoricColumn[T](column.elem.map(_.as(name.value)), name)
}

object DoricColumn extends ColGetters[NamedDoricColumn] {

  private[doric] def apply[T](dcolumn: Doric[Column]): DoricColumn[T] =
    TransformationDoricColumn(dcolumn)

  override protected def constructSide[T](
      column: Doric[Column],
      colName: CName
  ): NamedDoricColumn[T] = NamedDoricColumn(column, colName)

  private[doric] def uncheckedTypeAndExistence[T](
      col: Column
  ): DoricColumn[T] = {
    Kleisli[DoricValidated, Dataset[_], Column]((_: Dataset[_]) => col.valid)
  }.toDC

  def apply[T: SparkType](
      column: Column
  )(implicit location: Location): DoricColumn[T] =
    Kleisli[DoricValidated, Dataset[_], Column](df => {
      try {
        val head = df.select(column).schema.head
        if (SparkType[T].isEqual(head.dataType))
          Validated.valid(column)
        else
          ColumnTypeError(
            head.name.cname,
            SparkType[T].dataType,
            head.dataType
          ).invalidNec
      } catch {
        case e: Throwable => SparkErrorWrapper(e).invalidNec
      }
    }).toDC

  private[doric] def uncheckedType(column: Column): DoricColumn[_] = {
    Kleisli[DoricValidated, Dataset[_], Column](df => {
      try {
        df.select(column)
        Validated.valid(column)
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
