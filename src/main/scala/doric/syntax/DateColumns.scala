package doric
package syntax

import cats.implicits._
import doric.types.DateType

import org.apache.spark.sql.functions

trait DateColumns {
  implicit class DateColumnLikeSyntax[T: DateType](
      column: DoricColumn[T]
  ) {

    /**
      * Sets the moment to the last day of the same month.
      * @return
      */
    def endOfMonth: DoricColumn[T] = column.elem.map(functions.last_day).toDC

    /**
      * @return
      *   an Integer DoricColumn with the day number of the date.
      */
    def dayOfMonth: IntegerColumn = column.elem.map(functions.dayofmonth).toDC

    /**
      * Adds to the date the number of months
      * @param nMonths
      *   the number of months to add, can be negative to substract.
      * @return
      *   DoricColumn of the same type as the original with the month changed.
      */
    def addMonths(nMonths: IntegerColumn): DoricColumn[T] =
      (column.elem, nMonths.elem).mapN(functions.add_months).toDC
  }
}
