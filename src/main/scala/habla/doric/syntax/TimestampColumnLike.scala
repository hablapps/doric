package habla.doric
package syntax

import org.apache.spark.sql.functions
import habla.doric.FromDf

case class TimestampColumnLike[T]() {
    def hour(col: T)(implicit t: FromDf[T]): IntegerColumn = {
      IntegerColumn(functions.hour(col.sparkColumn))
    }

    def to_date(col: T)(implicit t: FromDf[T]): DateColumn = {
      DateColumn(functions.to_date(col.sparkColumn))
    }

    def add_months(col: T, numMonths: IntegerColumn)(implicit t: FromDf[T]): T =
      implicitly[FromDf[T]].construct(functions.add_months(col.sparkColumn, numMonths.sparkColumn))
}

trait TimestampColumnLikeOps {
  implicit class TimestampColumnLikeSyntax[T:  TimestampColumnLike: FromDf](column: T) {
    def hour: IntegerColumn = implicitly[TimestampColumnLike[T]].hour(column)

    def toDate: DateColumn = implicitly[TimestampColumnLike[T]].to_date(column)

    def addMonths(numMonths: IntegerColumn): T = implicitly[TimestampColumnLike[T]].add_months(column, numMonths)
  }
}
