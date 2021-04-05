package habla.doric
package syntax

import org.apache.spark.sql.functions

case class TimestampColumnLike[T]() {
  def hour(col: DoricColumn[T]): IntegerColumn = {
    col.map(functions.hour)
  }

  def to_date(col: DoricColumn[T]): DateColumn = {
    col.map(functions.to_date)
  }

  def add_months(col: DoricColumn[T], numMonths: IntegerColumn): DoricColumn[T] =
    col.mapN(numMonths)(functions.add_months)
}

object TimestampColumnLike {
  @inline def apply[T: TimestampColumnLike]: TimestampColumnLike[T] =
    implicitly[TimestampColumnLike[T]]
}

trait TimestampColumnLikeOps {
  implicit class TimestampColumnLikeSyntax[T: TimestampColumnLike](column: DoricColumn[T]) {
    def hour: IntegerColumn = TimestampColumnLike[T].hour(column)

    def toDate: DateColumn = TimestampColumnLike[T].to_date(column)

    def addMonths(numMonths: IntegerColumn): DoricColumn[T] =
      TimestampColumnLike[T].add_months(column, numMonths)
  }
}
