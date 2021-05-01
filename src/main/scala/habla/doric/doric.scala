package habla

import cats.data.{Kleisli, ValidatedNec}
import cats.implicits._
import cats.Applicative
import habla.doric.syntax._
import habla.doric.types.DoricAllTypes

import org.apache.spark.sql.{Column, DataFrame}

package object doric extends DoricAllTypes with AllSyntax {

  type DoricValidated[T] = ValidatedNec[DoricSingleError, T]
  type Doric[T]          = Kleisli[DoricValidated, DataFrame, T]
  type DoricJoin[T]      = Kleisli[DoricValidated, (DataFrame, DataFrame), T]

  implicit class DoricColumnops(elem: Doric[Column]) {
    def toDC[A]: DoricColumn[A] = DoricColumn(elem)
  }

  case class DoricColumn[T](elem: Doric[Column])

  object DoricColumn {
    def apply[T](f: DataFrame => DoricValidated[Column]): DoricColumn[T] =
      DoricColumn(Kleisli(f))

    def apply[T](col: Column): DoricColumn[T] = {
      Kleisli[DoricValidated, DataFrame, Column]((_: DataFrame) => col.valid)
    }.toDC
  }

  object DoricColumnExtr {
    def unapply[A: FromDf](
        column: Column
    )(implicit ap: Applicative[Doric]): Option[DoricColumn[A]] = {
      if (FromDf[A].isValid(column.expr.dataType))
        Some(column.pure[Doric].toDC)
      else
        None
    }
  }
}
