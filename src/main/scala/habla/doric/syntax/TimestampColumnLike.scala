package habla.doric
package syntax

import cats.implicits._

import org.apache.spark.sql.functions

trait TimestampColumnLike[T] {
  def hour(col: DoricColumn[T]): IntegerColumn = {
    (col.elem).map(functions.hour).toDC
  }

  def to_date(col: DoricColumn[T]): DateColumn = {
    (col.elem).map(functions.to_date).toDC
  }

  def add_months(col: DoricColumn[T], numMonths: IntegerColumn): DoricColumn[T] =
    (col.elem, numMonths.elem).mapN(functions.add_months).toDC
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
