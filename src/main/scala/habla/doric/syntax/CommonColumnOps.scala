package habla.doric
package syntax

import cats.data.{Kleisli, Validated}
import cats.implicits._

import org.apache.spark.sql.{Column, Dataset}

trait CommonColumnOps {

  implicit class SparkCol(private val column: Column) {
    def asDoric[T: SparkType](implicit location: Location): DoricColumn[T] =
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
  }

  implicit class BasicCol[T: SparkType](private val column: DoricColumn[T]) {

    type CastToT[To]  = Casting[T, To]
    type WCastToT[To] = UnsafeCasting[T, To]

    def as(colName: String): DoricColumn[T] = column.elem.map(_ as colName).toDC

    def ===(other: DoricColumn[T]): BooleanColumn =
      (column.elem, other.elem).mapN(_ === _).toDC

    def pipe[O](f: DoricColumn[T] => DoricColumn[O]): DoricColumn[O] = f(column)

    def cast[To: CastToT: SparkType]: DoricColumn[To] =
      Casting[T, To].cast(column)

    def unsafeCast[To: WCastToT: SparkType]: DoricColumn[To] =
      UnsafeCasting[T, To].cast(column)

    def isIn(elems: T*): BooleanColumn = column.elem.map(_.isin(elems: _*)).toDC

    def isNull: BooleanColumn    = column.elem.map(_.isNull).toDC
    def isNotNull: BooleanColumn = column.elem.map(_.isNotNull).toDC
    def isNaN: BooleanColumn     = column.elem.map(_.isNaN).toDC

  }

}
