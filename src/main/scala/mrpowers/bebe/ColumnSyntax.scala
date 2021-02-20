package mrpowers.bebe

import org.apache.spark.sql.Column

object ColumnSyntax {
  @inline def sparkFunction[T: ToColumn, O: FromDf](column: T, other: T, f: (Column, Column) => Column): O =
    construct[O](f(column.sparkColumn, other.sparkColumn))
}
