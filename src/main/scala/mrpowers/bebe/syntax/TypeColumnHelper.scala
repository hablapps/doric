package mrpowers.bebe
package syntax

import org.apache.spark.sql.Column

private[bebe] object TypeColumnHelper {
  @inline def sparkFunction[T: ToColumn, O: FromDf](
      column: T,
      other: T,
      f: (Column, Column) => Column
  ): O =
    construct[O](f(column.sparkColumn, other.sparkColumn))
}
