package habla.doric
package syntax

import org.apache.spark.sql.functions
import java.sql.Date

trait DateColumnLike[T] {

  def end_of_month(col: DoricColumn[T]): DoricColumn[T] =
    DoricColumn(functions.last_day(col.col))

  def day_of_month(col: DoricColumn[T]): IntegerColumn =
    DoricColumn(org.apache.spark.sql.functions.dayofmonth(col.col))

  def add_months(col: DoricColumn[T], nMonths: IntegerColumn): DoricColumn[T] =
    DoricColumn(functions.add_months(col.col, nMonths.col))

}

object DateColumnLike {
  @inline def apply[T: DateColumnLike]():DateColumnLike[T] = implicitly[DateColumnLike[T]]
}

trait DateColumnLikeOps {
  implicit class DateColumnLikeSyntax[T: DateColumnLike](column: DoricColumn[T]) {

    type IntLit[LT] = Literal[Int, LT]

    def endOfMonth: DoricColumn[T] = DateColumnLike[T].end_of_month(column)

    def dayOfMonth: IntegerColumn = DateColumnLike[T].day_of_month(column)

    def addMonths(nMonths: IntegerColumn): DoricColumn[T] = DateColumnLike[T].add_months(column, nMonths)
    
    def addMonths[LT: IntLit](nMonths: LT): DoricColumn[T] = DateColumnLike[T].add_months(column, nMonths.lit)
  }
}
