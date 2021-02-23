package mrpowers.bebe
package syntax

trait CommonColumnOps {

  implicit class BasicCol[T: FromDf : ToColumn](val column: T) {

    def as(colName: String): T = construct(column.sparkColumn as colName)

    def ===(other: T): BooleanColumn = BooleanColumn(column.sparkColumn === other.sparkColumn)

    def pipe[O: ToColumn](f: T => O): O = f(column)
  }

}
