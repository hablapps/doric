package doric
package syntax

import doric.types.TimestampType

import org.apache.spark.sql.functions

private[syntax] trait TimestampColumns {
  implicit class TimestampColumnLikeSyntax[T: TimestampType](
      column: DoricColumn[T]
  ) {

    /**
      * @group Timestamp Type
      * @return
      *   the hour of the time.
      */
    def hour: IntegerColumn = column.elem.map(functions.hour).toDC

    /**
      * @group Timestamp Type
      * @return
      *   a Date Column without the hour
      */
    def toDate: DateColumn = column.elem.map(functions.to_date).toDC

    /**
      * @group Timestamp Type
      * @return
      *   a LocalDate Column without the hour
      */
    def toLocalDate: LocalDateColumn = column.elem.map(functions.to_date).toDC
  }
}
