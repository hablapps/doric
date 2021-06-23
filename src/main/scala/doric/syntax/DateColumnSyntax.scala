package doric
package syntax

import cats.implicits._
import doric.types.DateType

import org.apache.spark.sql.functions

trait DateColumnSyntax {
  implicit class DateColumnLikeSyntax[T: DateType](
      column: DoricColumn[T]
  ) {

    def endOfMonth: DoricColumn[T] = column.elem.map(functions.last_day).toDC

    def dayOfMonth: IntegerColumn = column.elem.map(functions.dayofmonth).toDC

    def addMonths(nMonths: IntegerColumn): DoricColumn[T] =
      (column.elem, nMonths.elem).mapN(functions.add_months).toDC

  }
}
