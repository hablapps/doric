package mrpowers

import java.sql.{Date, Timestamp}
import mrpowers.bebe.syntax.{NumericOperationsOps, DataFrameOps, FromDfExtras, LiteralConversions, NumericOperations, ToColumnExtras}

import org.apache.spark.sql.{functions, Column}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._


package object bebe extends ToColumnExtras with FromDfExtras with DataFrameOps with NumericOperationsOps with LiteralConversions {

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

  trait StaticColumnOps {

    /**
      * The type of the typed column
      */
    type T

    /**
      * The type in scala that corresponds the typed column
      */
    type BT

    /**
      * The spark type for the previous types
      *
      * @return
      */
    def sparkType: DataType

    def createCol(column: Column): T

    def getCol(tc: T): Column

    def apply(litValue: BT): T = createCol(lit(litValue))

    def unapply(column: Column): Option[T] = {
      if (column.expr.dataType == sparkType)
        Some(createCol(column))
      else
        None
    }

    implicit val fromDf: FromDf[T] = new FromDf[T] {
      override def dataType: DataType = sparkType

      override def construct(column: Column): T = createCol(column)
    }

    implicit val toColumn: ToColumn[T] = (typedCol: T) => getCol(typedCol)

  }

  case class TimestampColumn(col: Column) extends TimestampColumnLike with DateOrTimestampColumnLike

  object TimestampColumn extends StaticColumnOps {

    type T = TimestampColumn
    type BT = Timestamp

    override def sparkType: DataType = TimestampType

    override def createCol(column: Column): TimestampColumn = TimestampColumn(column)

    override def getCol(tc: TimestampColumn): Column = tc.col
  }


  case class DateColumn(col: Column) extends DateColumnLike with DateOrTimestampColumnLike

  object DateColumn extends StaticColumnOps {

    type T = DateColumn
    type BT = Date

    override def sparkType: DataType = DateType

    override def createCol(column: Column): DateColumn = DateColumn(column)

    override def getCol(tc: DateColumn): Column = tc.col
  }


  case class IntegerColumn(col: Column)

  object IntegerColumn extends StaticColumnOps {

    override type T = IntegerColumn
    override type BT = Int

    override def sparkType: DataType = IntegerType

    override def createCol(column: Column): IntegerColumn = IntegerColumn(column)

    override def getCol(tc: IntegerColumn): Column = tc.col

    implicit val intArith: NumericOperations[IntegerColumn] = NumericOperations[IntegerColumn]()

  }

  case class BooleanColumn(col: Column)

  object BooleanColumn extends StaticColumnOps {

    override type T = BooleanColumn
    override type BT = Boolean

    override def sparkType: DataType = BooleanType

    override def createCol(column: Column): BooleanColumn = BooleanColumn(column)

    override def getCol(tc: BooleanColumn): Column = tc.col
  }

  case class StringColumn(col: Column)

  object StringColumn extends StaticColumnOps {

    override type T = StringColumn
    override type BT = String

    override def sparkType: DataType = StringType

    override def createCol(column: Column): StringColumn = StringColumn(column)

    override def getCol(tc: StringColumn): Column = tc.col
  }

}
