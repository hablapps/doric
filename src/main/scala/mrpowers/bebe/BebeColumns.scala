package mrpowers.bebe

import org.apache.spark.sql.{functions, Column, DataFrame}
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, TimestampType}

object Columns {

  trait FromDf[T] {
    val dataType: DataType

    def construct(column: Column): T

    def validate(df: DataFrame, colName: String): T = {
      val column = df(colName)
      if (column.expr.dataType == dataType)
        construct(column)
      else
        throw new Exception(s"The column $colName in the dataframe is of type ${column.expr.dataType} and it was expected to be $dataType")
    }
  }

  trait ToColumn[T] {
    def column(typedCol: T): Column
  }

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

    def unapply(column: Column): Option[TimestampColumn] = {
      if (column.expr.dataType == TimestampType)
        Some(implicitly[FromDf[TimestampColumn]].construct(column))
      else
        None
    }

    implicit val timestampFromDf: FromDf[TimestampColumn] = new FromDf[TimestampColumn] {
      override val dataType: DataType = TimestampType

      override def construct(column: Column): TimestampColumn = TimestampColumn(column)
    }

    implicit val timestampColumn: ToColumn[TimestampColumn] = (typedCol: TimestampColumn) => typedCol.col
  }


  case class DateColumn(col: Column) extends DateColumnLike with DateOrTimestampColumnLike

  object DateColumn {
    def apply(strCol: String): DateColumn = DateColumn(org.apache.spark.sql.functions.col(strCol))

    def unapply(column: Column): Option[DateColumn] = {
      if (column.expr.dataType == DateType)
        Some(implicitly[FromDf[DateColumn]].construct(column))
      else
        None
    }

    implicit val integerFromDf: FromDf[DateColumn] = new FromDf[DateColumn] {
      override val dataType: DataType = DateType

      override def construct(column: Column): DateColumn = DateColumn(column)
    }

    implicit val dateColumn: ToColumn[DateColumn] = (typedCol: DateColumn) => typedCol.col
  }


  case class IntegerColumn(col: Column)

  object IntegerColumn {
    def apply(strCol: String): IntegerColumn = IntegerColumn(org.apache.spark.sql.functions.col(strCol))

    def apply(intCol: Int): IntegerColumn = IntegerColumn(org.apache.spark.sql.functions.lit(intCol))

    def unapply(column: Column): Option[IntegerColumn] = {
      if (column.expr.dataType == IntegerType)
        Some(implicitly[FromDf[IntegerColumn]].construct(column))
      else
        None
    }

    implicit val integerFromDf: FromDf[IntegerColumn] = new FromDf[IntegerColumn] {
      override val dataType: DataType = IntegerType

      override def construct(column: Column): IntegerColumn = IntegerColumn(column)
    }

    implicit val integerColumn: ToColumn[IntegerColumn] = (typedCol: IntegerColumn) => typedCol.col
  }

}
