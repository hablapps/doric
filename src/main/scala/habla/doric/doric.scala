package habla

import cats.data.{Kleisli, ValidatedNec}
import cats.implicits._
import cats.Applicative
import habla.doric.syntax._
import habla.doric.types.DoricAllTypes
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.{Column, Dataset}

package object doric extends DoricAllTypes with AllSyntax {

  type DoricValidated[T] = ValidatedNec[DoricSingleError, T]
  type Doric[T]          = Kleisli[DoricValidated, Dataset[_], T]
  type DoricJoin[T]      = Kleisli[DoricValidated, (Dataset[_], Dataset[_]), T]

  implicit class DoricColumnops(elem: Doric[Column]) {
    def toDC[A]: DoricColumn[A] = DoricColumn(elem)
  }

  case class DoricColumn[T](elem: Doric[Column])

  object DoricColumn {
    def apply[T](f: Dataset[_] => DoricValidated[Column]): DoricColumn[T] =
      DoricColumn(Kleisli(f))

    def apply[T](col: Column): DoricColumn[T] = {
      Kleisli[DoricValidated, Dataset[_], Column]((_: Dataset[_]) => col.valid)
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

  case class DoricJoinColumn(elem: DoricJoin[Column]) {
    def &&(other: DoricJoinColumn): DoricJoinColumn =
      (elem, other.elem).mapN(_ && _).toDJC
  }

  implicit class DoricJoinColumnOps(elem: DoricJoin[Column]) {
    def toDJC: DoricJoinColumn = DoricJoinColumn(elem)
  }

  case class LeftDoricColumn[T](elem: Doric[Column]) {
    def ===(right: RightDoricColumn[T]): DoricJoinColumn =
      Kleisli[DoricValidated, (Dataset[_], Dataset[_]), Column](dfs => {
        (elem.run(dfs._1), right.elem.run(dfs._2)).mapN(_ === _)
      }).toDJC

  }

  case class RightDoricColumn[T](elem: Doric[Column]) {
    def ===(left: LeftDoricColumn[T]): DoricJoinColumn =
      Kleisli[DoricValidated, (Dataset[_], Dataset[_]), Column](dfs => {
        (left.elem.run(dfs._1), elem.run(dfs._2)).mapN(_ === _)
      }).toDJC
  }

  object LeftDF {
    def col[T: FromDf](colName: String)(implicit
        location: Location
    ): LeftDoricColumn[T] =
      LeftDoricColumn(FromDfExtras.col[T](colName).elem)

    def colString(colName: String)(implicit
        location: Location
    ): LeftDoricColumn[String] =
      LeftDoricColumn(FromDfExtras.col[String](colName).elem)
    def colInt(colName: String)(implicit
        location: Location
    ): LeftDoricColumn[Int] =
      LeftDoricColumn(FromDfExtras.col[Int](colName).elem)
    def colLong(colName: String)(implicit
        location: Location
    ): LeftDoricColumn[Long] =
      LeftDoricColumn(FromDfExtras.col[Long](colName).elem)
    def colInstant(colName: String)(implicit
        location: Location
    ): LeftDoricColumn[Instant] =
      LeftDoricColumn(FromDfExtras.col[Instant](colName).elem)
    def colLocalDate(colName: String)(implicit
        location: Location
    ): LeftDoricColumn[LocalDate] =
      LeftDoricColumn(FromDfExtras.col[LocalDate](colName).elem)

    def apply[T](doricColumn: DoricColumn[T]): LeftDoricColumn[T] =
      LeftDoricColumn(doricColumn.elem)
  }

  object RightDF {
    def col[T: FromDf](colName: String)(implicit
        location: Location
    ): RightDoricColumn[T] =
      RightDoricColumn(FromDfExtras.col[T](colName).elem)

    def colString(colName: String)(implicit
        location: Location
    ): RightDoricColumn[String] =
      RightDoricColumn(FromDfExtras.col[String](colName).elem)
    def colInt(colName: String)(implicit
        location: Location
    ): RightDoricColumn[Int] =
      RightDoricColumn(FromDfExtras.col[Int](colName).elem)
    def colLong(colName: String)(implicit
        location: Location
    ): RightDoricColumn[Long] =
      RightDoricColumn(FromDfExtras.col[Long](colName).elem)
    def colInstant(colName: String)(implicit
        location: Location
    ): RightDoricColumn[Instant] =
      RightDoricColumn(FromDfExtras.col[Instant](colName).elem)
    def colLocalDate(colName: String)(implicit
        location: Location
    ): RightDoricColumn[LocalDate] =
      RightDoricColumn(FromDfExtras.col[LocalDate](colName).elem)

    def apply[T](doricColumn: DoricColumn[T]): RightDoricColumn[T] =
      RightDoricColumn(doricColumn.elem)
  }
}
