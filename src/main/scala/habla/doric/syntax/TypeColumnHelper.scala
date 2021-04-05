package habla.doric
package syntax

import org.apache.spark.sql.Column
import cats.implicits._

private[doric] object TypeColumnHelper {
  @inline def sparkFunction[T, O](
      column: DoricColumn[T],
      other: DoricColumn[T],
      f: (Column, Column) => Column
  ): DoricColumn[O] =
    DoricColumn((column.toKleisli, other.toKleisli).mapN(f).run)
}
