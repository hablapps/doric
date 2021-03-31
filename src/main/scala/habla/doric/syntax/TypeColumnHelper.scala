package habla.doric
package syntax

import org.apache.spark.sql.Column

private[doric] object TypeColumnHelper {
  @inline def sparkFunction[T, O](
      column: DoricColumn[T],
      other: DoricColumn[T],
      f: (Column, Column) => Column
  ): DoricColumn[O] =
    DoricColumn(f(column.col, other.col))
}
