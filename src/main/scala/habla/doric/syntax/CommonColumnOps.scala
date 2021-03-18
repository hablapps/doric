package habla.doric
package syntax

trait CommonColumnOps {

  implicit class BasicCol[T: FromDf](val column: T) {

    type CastToT[To] = Casting[T, To]

    def as(colName: String): T = construct(column.sparkColumn as colName)

    def ===(other: T): BooleanColumn = BooleanColumn(column.sparkColumn === other.sparkColumn)

    def pipe[O: FromDf](f: T => O): O = f(column)

    def castTo[To: CastToT: FromDf]: To = implicitly[Casting[T, To]].cast(column)

  }

}
