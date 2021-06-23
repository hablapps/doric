package doric
package syntax

import cats.implicits._
import doric.sem.Location
import doric.types.{Casting, SparkType, UnsafeCasting}

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

trait CommonColumnSyntax extends ColGetters[DoricColumn] {

  @inline def dataType[T: SparkType]: DataType = SparkType[T].dataType

  override protected def constructSide[T](
      column: Doric[Column]
  ): DoricColumn[T] =
    DoricColumn(column)

  implicit class SparkCol(private val column: Column) {
    @inline def asDoric[T: SparkType](implicit
        location: Location
    ): DoricColumn[T] =
      DoricColumn(column)
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
