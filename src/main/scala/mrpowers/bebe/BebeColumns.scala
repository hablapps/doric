package mrpowers.bebe

import org.apache.spark.sql.{functions, Column, DataFrame}
import org.apache.spark.sql.types.{DataType, DateType, IntegerType}

object Columns {

  trait FromDf[T] {
    val dataType: DataType

    def construct(column: Column): T

    def validate(df: DataFrame, colName: String): T = {
      val column = df(colName)
      val dfDatatype = df.schema.find(_.name == colName).get.dataType
      if (dfDatatype == dataType)
        construct(column)
      else
        throw new Exception(s"The column $colName in the dataframe is of type $dfDatatype and it was expected to be $dataType")
    }
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
  }


  case class DateColumn(col: Column) extends DateColumnLike with DateOrTimestampColumnLike

  object DateColumn {
    def apply(strCol: String): DateColumn = DateColumn(org.apache.spark.sql.functions.col(strCol))

    implicit val integerFromDf: FromDf[DateColumn] = new FromDf[DateColumn] {
      override val dataType: DataType = DateType

      override def construct(column: Column): DateColumn = DateColumn(column)
    }
  }


  case class IntegerColumn(col: Column)

  object IntegerColumn {
    def apply(strCol: String): IntegerColumn = IntegerColumn(org.apache.spark.sql.functions.col(strCol))

    def apply(intCol: Int): IntegerColumn = IntegerColumn(org.apache.spark.sql.functions.lit(intCol))

    implicit val integerFromDf: FromDf[IntegerColumn] = new FromDf[IntegerColumn] {
      override val dataType: DataType = IntegerType

      override def construct(column: Column): IntegerColumn = IntegerColumn(column)
    }
  }

}
