package habla.doric
package syntax

import cats.implicits._

trait CommonColumnOps {

  implicit class BasicCol[T](private val column: DoricColumn[T]) {

    type CastToT[To]  = Casting[T, To]
    type WCastToT[To] = WarningCasting[T, To]
    type Lit[ST]      = Literal[T, ST]

    def as(colName: String): DoricColumn[T] = DoricColumn(column.toKleisli.map(_ as colName).run)

    def ===(other: DoricColumn[T]): BooleanColumn = DoricColumn(
      (column.toKleisli, other.toKleisli).mapN(_ === _).run
    )

    def ===[LT: Lit](other: LT): BooleanColumn = column === other.lit

    def pipe[O](f: DoricColumn[T] => DoricColumn[O]): DoricColumn[O] = f(column)

    def castTo[To: CastToT: FromDf]: DoricColumn[To] = Casting[T, To].cast(column)

    def warningCastTo[To: WCastToT: FromDf]: DoricColumn[To] = WarningCasting[T, To].cast(column)

  }

}
