package habla.doric
package syntax

trait CommonColumnOps {

  implicit class BasicCol[T](private val column: DoricColumn[T]) {

    type CastToT[To]  = Casting[T, To]
    type WCastToT[To] = WarningCasting[T, To]
    type Lit[ST]      = Literal[T, ST]

    def as(colName: String): DoricColumn[T] = DoricColumn(column.col as colName)

    def ===(other: DoricColumn[T]): BooleanColumn = DoricColumn(column.col === other.col)

    def ===[LT: Lit](other: LT): BooleanColumn = column === other.lit

    def pipe[O](f: DoricColumn[T] => DoricColumn[O]): DoricColumn[O] = f(column)

    def castTo[To: CastToT: FromDf]: DoricColumn[To] = Casting[T, To].cast(column)

    def warningCastTo[To: WCastToT: FromDf]: DoricColumn[To] = WarningCasting[T, To].cast(column)

  }

}
