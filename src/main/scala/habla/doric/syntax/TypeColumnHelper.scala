package habla.doric
package syntax

import org.apache.spark.sql.Column

private[doric] object TypeColumnHelper {
  @inline def sparkFunction[T: FromDf, O: FromDf](
      column: T,
      other: T,
      f: (Column, Column) => Column
  ): O =
    construct[O](f(column.sparkColumn, other.sparkColumn))
}
