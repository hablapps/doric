package habla.doric
package syntax

import org.apache.spark.sql.functions

trait DateColumnLike[T] {

  def end_of_month(col: T)(implicit fromdf: FromDf[T]): T =
    fromdf.construct(functions.last_day(col.sparkColumn))

  def day_of_month(col: T)(implicit fromdf: FromDf[T]): IntegerColumn =
    IntegerColumn(org.apache.spark.sql.functions.dayofmonth(col.sparkColumn))

  def add_months(col: T, nMonths: IntegerColumn)(implicit fromdf: FromDf[T]): T =
    fromdf.construct(functions.add_months(col.sparkColumn, nMonths.sparkColumn))

}

trait DateColumnLikeOps {
  implicit class DateColumnLikeSyntax[T: DateColumnLike: FromDf](column: T) {

    type IntLit[LT] = Literal[IntegerColumn, LT]

    def endOfMonth: T = implicitly[DateColumnLike[T]].end_of_month(column)

    def dayOfMonth: IntegerColumn = implicitly[DateColumnLike[T]].day_of_month(column)

    def addMonths(nMonths: IntegerColumn): T = implicitly[DateColumnLike[T]].add_months(column, nMonths)
    
    def addMonths[LT: IntLit](nMonths: LT)(implicit fromdfInt: FromDf[IntegerColumn]): T = implicitly[DateColumnLike[T]].add_months(column, nMonths.lit[IntegerColumn])
  }
}
