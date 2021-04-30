package habla.doric
package syntax

import cats.implicits._

import org.apache.spark.sql.functions

trait DateColumnLike[T] {

  def end_of_month(col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(functions.last_day).toDC

  def day_of_month(col: DoricColumn[T]): IntegerColumn =
    col.elem.map(functions.dayofmonth).toDC

  def add_months(col: DoricColumn[T], nMonths: IntegerColumn): DoricColumn[T] =
    (col.elem, nMonths.elem).mapN(functions.add_months).toDC

}

object DateColumnLike {
  @inline def apply[T: DateColumnLike]: DateColumnLike[T] =
    implicitly[DateColumnLike[T]]
}

trait DateColumnLikeOps {
  implicit class DateColumnLikeSyntax[T: DateColumnLike](
      column: DoricColumn[T]
  ) {

    def endOfMonth: DoricColumn[T] = DateColumnLike[T].end_of_month(column)

    def dayOfMonth: IntegerColumn = DateColumnLike[T].day_of_month(column)

    def addMonths(nMonths: IntegerColumn): DoricColumn[T] =
      DateColumnLike[T].add_months(column, nMonths)

  }
}
