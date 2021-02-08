package mrpowers.bebe

import org.apache.spark.sql.{functions, Column}

object Columns {

  trait DateOrTimestampColumnLike {
    val col: Column

    def day_of_month: IntegerColumn = IntegerColumn(org.apache.spark.sql.functions.dayofmonth(col))
  }


  trait TimestampColumnLike {
    val col: Column

    def hour: IntegerColumn = {
      IntegerColumn(functions.hour(col))
    }

    def to_date: DateColumn = {
      DateColumn(functions.to_date(col))
    }

    def add_months(numMonths: IntegerColumn): TimestampColumn = TimestampColumn(functions.add_months(col, numMonths.col))
  }


  trait DateColumnLike {
    val col: Column

    def end_of_month: DateColumn = DateColumn(functions.last_day(col))

    def add_months(numMonths: IntegerColumn): DateColumn = DateColumn(functions.add_months(col, numMonths.col))
  }


  case class TimestampColumn(col: Column) extends TimestampColumnLike with DateOrTimestampColumnLike

  object TimestampColumn {
    def apply(strCol: String): TimestampColumn = TimestampColumn(org.apache.spark.sql.functions.col(strCol))
  }


  case class DateColumn(col: Column) extends DateColumnLike with DateOrTimestampColumnLike

  object DateColumn {
    def apply(strCol: String): DateColumn = DateColumn(org.apache.spark.sql.functions.col(strCol))
  }


  case class IntegerColumn(col: Column)

  object IntegerColumn {
    def apply(strCol: String): IntegerColumn = IntegerColumn(org.apache.spark.sql.functions.col(strCol))
    def apply(intCol: Int): IntegerColumn = IntegerColumn(org.apache.spark.sql.functions.lit(intCol))
  }

}
