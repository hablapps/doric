package habla.doric
package syntax

import org.apache.spark.sql.functions
import java.sql.Date

trait DateColumnLike[T] {

  def end_of_month(col: DoricColumn[T]): DoricColumn[T] =
    col.map(functions.last_day)

  def day_of_month(col: DoricColumn[T]): IntegerColumn =
    col.map(functions.dayofmonth)

  def add_months(col: DoricColumn[T], nMonths: IntegerColumn): DoricColumn[T] =
    col.mapN(nMonths)(functions.add_months)

}

object DateColumnLike {
  @inline def apply[T: DateColumnLike]: DateColumnLike[T] = implicitly[DateColumnLike[T]]
}

trait DateColumnLikeOps {
  implicit class DateColumnLikeSyntax[T: DateColumnLike](column: DoricColumn[T]) {

    type IntLit[LT] = Literal[Int, LT]

    def endOfMonth: DoricColumn[T] = DateColumnLike[T].end_of_month(column)

    def dayOfMonth: IntegerColumn = DateColumnLike[T].day_of_month(column)

    def addMonths(nMonths: IntegerColumn): DoricColumn[T] =
      DateColumnLike[T].add_months(column, nMonths)

    def addMonths[LT: IntLit](nMonths: LT): DoricColumn[T] =
      DateColumnLike[T].add_months(column, nMonths.lit)
  }
}
