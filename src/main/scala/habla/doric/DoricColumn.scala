package habla.doric

import cats.data.{Kleisli, NonEmptyChain, Validated}
import cats.implicits.{catsSyntaxTuple2Semigroupal, catsSyntaxValidatedId, catsSyntaxValidatedIdBinCompat0}
import habla.doric.sem.{ColumnTypeError, DoricSingleError, Location, SparkErrorWrapper}
import habla.doric.syntax.ColGetters
import habla.doric.types.SparkType

import org.apache.spark.sql.{Column, Dataset}

case class DoricColumn[T](elem: Doric[Column])

object DoricColumn extends ColGetters[DoricColumn] {
  override protected def constructSide[T](
      column: Doric[Column]
  ): DoricColumn[T] = DoricColumn(column)

  private[doric] def unchecked[T](col: Column): DoricColumn[T] = {
    Kleisli[DoricValidated, Dataset[_], Column]((_: Dataset[_]) => col.valid)
  }.toDC

  def apply[T: SparkType](
      column: Column
  )(implicit location: Location): DoricColumn[T] =
    Kleisli[DoricValidated, Dataset[_], Column](df => {
      try {
        val head = df.select(column).schema.head
        if (SparkType[T].isValid(head.dataType))
          Validated.valid(column)
        else
          ColumnTypeError(
            head.name,
            SparkType[T].dataType,
            head.dataType
          ).invalidNec
      } catch {
        case e: Throwable => SparkErrorWrapper(e).invalidNec
      }
    }).toDC

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
