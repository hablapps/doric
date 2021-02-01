package mrpowers.bebe

import org.apache.spark.sql.Column

object Columns {

  trait ColumnLike {
    val colLike: Any

    val col = colLike match {
      case v: String => org.apache.spark.sql.functions.col(v)
      case v: Column => v
    }
  }


  trait DateOrTimestampColumnLike {
    val col: Column

    def day_of_month: IntegerColumn = IntegerColumn(org.apache.spark.sql.functions.dayofmonth(col))
  }


  trait TimestampColumnLike {
    val col: Column

    def hour: IntegerColumn = {
      IntegerColumn(org.apache.spark.sql.functions.hour(col))
    }

    def to_date: DateColumn = {
      DateColumn(org.apache.spark.sql.functions.to_date(col))
    }
  }


  trait DateColumnLike {
    val col: Column

    def end_of_month: DateColumn = DateColumn(org.apache.spark.sql.functions.last_day(col))
  }


  case class TimestampColumn(colLike: Any) extends TimestampColumnLike with DateOrTimestampColumnLike with ColumnLike


  case class DateColumn(colLike: Any) extends DateColumnLike with DateOrTimestampColumnLike with ColumnLike


  case class IntegerColumn(colLike: Any) {
    val col = colLike match {
      case v: String => org.apache.spark.sql.functions.col(v)
      case v: Int => org.apache.spark.sql.functions.lit(v)
      case v: Column => v
    }
  }


}
