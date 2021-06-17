package habla.doric
package syntax

import cats.implicits._
import habla.doric.types.TimestampType

import org.apache.spark.sql.functions

trait TimestampSyntax {
  implicit class TimestampColumnLikeSyntax[T: TimestampType](
      column: DoricColumn[T]
  ) {
    def hour: IntegerColumn = column.elem.map(functions.hour).toDC

    def toDate: DateColumn = column.elem.map(functions.to_date).toDC

    def addMonths(numMonths: IntegerColumn): DoricColumn[T] =
      (column.elem, numMonths.elem).mapN(functions.add_months).toDC
  }
}
