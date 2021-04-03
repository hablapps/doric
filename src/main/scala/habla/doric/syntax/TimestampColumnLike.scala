package habla.doric
package syntax

import org.apache.spark.sql.functions
import habla.doric.FromDf
import java.sql.Time

case class TimestampColumnLike[T]() {
  def hour(col: DoricColumn[T])(implicit t: FromDf[T]): IntegerColumn = {
    DoricColumn(functions.hour(col.col))
  }

  def to_date(col: DoricColumn[T])(implicit t: FromDf[T]): DateColumn = {
    DoricColumn(functions.to_date(col.col))
  }

  def add_months(col: DoricColumn[T], numMonths: IntegerColumn)(implicit
      t: FromDf[T]
  ): DoricColumn[T] =
    DoricColumn(functions.add_months(col.col, numMonths.col))
}

object TimestampColumnLike {
  @inline def apply[T: TimestampColumnLike]: TimestampColumnLike[T] =
    implicitly[TimestampColumnLike[T]]
}

trait TimestampColumnLikeOps {
  implicit class TimestampColumnLikeSyntax[T: TimestampColumnLike: FromDf](column: DoricColumn[T]) {
    def hour: IntegerColumn = TimestampColumnLike[T].hour(column)

    def toDate: DateColumn = TimestampColumnLike[T].to_date(column)

    def addMonths(numMonths: IntegerColumn): DoricColumn[T] =
      TimestampColumnLike[T].add_months(column, numMonths)
  }
}
