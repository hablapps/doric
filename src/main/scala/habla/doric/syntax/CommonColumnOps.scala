package habla.doric
package syntax

import cats.implicits._

trait CommonColumnOps {

  implicit class BasicCol[T](private val column: DoricColumn[T]) {

    type CastToT[To]  = Casting[T, To]
    type WCastToT[To] = WarningCasting[T, To]
    type Lit[ST]      = Literal[T, ST]

    def as(colName: String): DoricColumn[T] = column.elem.map(_ as colName).toDC

    def ===(other: DoricColumn[T]): BooleanColumn =
      (column.elem, other.elem).mapN(_ === _).toDC

    def ===[LT: Lit](other: LT): BooleanColumn = column === other.lit

    def pipe[O](f: DoricColumn[T] => DoricColumn[O]): DoricColumn[O] = f(column)

    def castTo[To: CastToT: FromDf]: DoricColumn[To] = Casting[T, To].cast(column)

    def warningCastTo[To: WCastToT: FromDf]: DoricColumn[To] = WarningCasting[T, To].cast(column)

    def isIn[LT: Lit](elems: LT*): BooleanColumn = column.elem.map(_.isin(elems: _*)).toDC

  }

}
