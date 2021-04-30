package habla.doric
package syntax

import cats.data.{Kleisli, Validated}
import cats.implicits._

import org.apache.spark.sql.{Column, DataFrame}

trait CommonColumnOps {

  implicit class SparkCol(private val column: Column) {
    def asDoric[T: FromDf](implicit location: Location): DoricColumn[T] =
      Kleisli[DoricValidated, DataFrame, Column](df => {
        try {
          val head = df.select(column).schema.head
          if (FromDf[T].isValid(head.dataType))
            Validated.valid(column)
          else
            ColumnTypeError(head.name, FromDf[T].dataType, head.dataType).invalidNec
        } catch {
          case e: Throwable => SparkErrorWrapper(e).invalidNec
        }
      }).toDC
  }

  implicit class BasicCol[T](private val column: DoricColumn[T]) {

    type CastToT[To]  = Casting[T, To]
    type WCastToT[To] = WarningCasting[T, To]

    def as(colName: String): DoricColumn[T] = column.elem.map(_ as colName).toDC

    def ===(other: DoricColumn[T]): BooleanColumn =
      (column.elem, other.elem).mapN(_ === _).toDC

    def pipe[O](f: DoricColumn[T] => DoricColumn[O]): DoricColumn[O] = f(column)

    def castTo[To: CastToT: FromDf]: DoricColumn[To] = Casting[T, To].cast(column)

    def warningCastTo[To: WCastToT: FromDf]: DoricColumn[To] = WarningCasting[T, To].cast(column)

    def isIn(elems: T*): BooleanColumn = column.elem.map(_.isin(elems: _*)).toDC

    def isNull: BooleanColumn    = column.elem.map(_.isNull).toDC
    def isNotNull: BooleanColumn = column.elem.map(_.isNotNull).toDC
    def isNaN: BooleanColumn     = column.elem.map(_.isNaN).toDC

  }

}
