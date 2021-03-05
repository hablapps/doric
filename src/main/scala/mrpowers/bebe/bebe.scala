package mrpowers

import java.sql.{Date, Timestamp}
import mrpowers.bebe.syntax.{
  CommonColumnOps,
  DataFrameOps,
  FromDfExtras,
  LiteralConversions,
  NumericOperations,
  NumericOperationsOps,
  ToColumnExtras
}

import org.apache.spark.sql.{functions, Column}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.parquet.filter2.predicate.Operators.LongColumn

package object bebe
    extends ToColumnExtras
    with FromDfExtras
    with DataFrameOps
    with NumericOperationsOps
    with LiteralConversions
    with CommonColumnOps {

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

    def add_months(numMonths: IntegerColumn): TimestampColumn =
      TimestampColumn(functions.add_months(col, numMonths.col))
  }

  trait DateColumnLike {
    val col: Column

    def end_of_month: DateColumn = DateColumn(functions.last_day(col))

    def add_months(numMonths: IntegerColumn): DateColumn =
      DateColumn(functions.add_months(col, numMonths.col))
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

    implicit val literal: Literal[T, BT] = new Literal[T, BT] {}

  }

  case class TimestampColumn(col: Column) extends TimestampColumnLike with DateOrTimestampColumnLike

  object TimestampColumn extends StaticColumnOps {

    type T  = TimestampColumn
    type BT = Timestamp

    override def sparkType: DataType = TimestampType

    override def createCol(column: Column): TimestampColumn = TimestampColumn(column)

    override def getCol(tc: TimestampColumn): Column = tc.col
  }

  case class DateColumn(col: Column) extends DateColumnLike with DateOrTimestampColumnLike

  object DateColumn extends StaticColumnOps {

    type T  = DateColumn
    type BT = Date

    override def sparkType: DataType = DateType

    override def createCol(column: Column): DateColumn = DateColumn(column)

    override def getCol(tc: DateColumn): Column = tc.col
  }

  case class IntegerColumn(col: Column)

  object IntegerColumn extends StaticColumnOps {

    override type T  = IntegerColumn
    override type BT = Int

    override def sparkType: DataType = IntegerType

    override def createCol(column: Column): IntegerColumn = IntegerColumn(column)

    override def getCol(tc: IntegerColumn): Column = tc.col

    implicit val intArith: NumericOperations[IntegerColumn] = NumericOperations[IntegerColumn]()

    implicit val castToString: Casting[IntegerColumn, StringColumn] =
      new SparkCasting[IntegerColumn, StringColumn] {}

    implicit val castToLong: Casting[IntegerColumn, LongColumn] =
      new SparkCasting[IntegerColumn, LongColumn] {}

    implicit val castToFloat: Casting[IntegerColumn, FloatColumn] =
      new SparkCasting[IntegerColumn, FloatColumn] {}

    implicit val castToDouble: Casting[IntegerColumn, DoubleColumn] =
      new SparkCasting[IntegerColumn, DoubleColumn] {}

  }

  case class LongColumn(col: Column)

  object LongColumn extends StaticColumnOps {

    override type T  = LongColumn
    override type BT = Int

    override def sparkType: DataType = LongType

    override def createCol(column: Column): LongColumn = LongColumn(column)

    override def getCol(tc: LongColumn): Column = tc.col

    implicit val intArith: NumericOperations[LongColumn] = NumericOperations[LongColumn]()

    implicit val castToString: Casting[LongColumn, StringColumn] =
      new SparkCasting[LongColumn, StringColumn] {}

    implicit val castToFloat: Casting[LongColumn, FloatColumn] =
      new SparkCasting[LongColumn, FloatColumn] {}

    implicit val castToDouble: Casting[LongColumn, DoubleColumn] =
      new SparkCasting[LongColumn, DoubleColumn] {}

  }

  case class FloatColumn(col: Column)

  object FloatColumn extends StaticColumnOps {

    override type T  = FloatColumn
    override type BT = Int

    override def sparkType: DataType = FloatType

    override def createCol(column: Column): FloatColumn = FloatColumn(column)

    override def getCol(tc: FloatColumn): Column = tc.col

    implicit val intArith: NumericOperations[FloatColumn] = NumericOperations[FloatColumn]()

    implicit val castToString: Casting[FloatColumn, StringColumn] =
      new SparkCasting[FloatColumn, StringColumn] {}

    implicit val castToDouble: Casting[FloatColumn, DoubleColumn] =
      new SparkCasting[FloatColumn, DoubleColumn] {}

  }

  case class DoubleColumn(col: Column)

  object DoubleColumn extends StaticColumnOps {

    override type T  = DoubleColumn
    override type BT = Int

    override def sparkType: DataType = DoubleType

    override def createCol(column: Column): DoubleColumn = DoubleColumn(column)

    override def getCol(tc: DoubleColumn): Column = tc.col

    implicit val intArith: NumericOperations[DoubleColumn] = NumericOperations[DoubleColumn]()

    implicit val castToString: Casting[DoubleColumn, StringColumn] =
      new SparkCasting[DoubleColumn, StringColumn] {}

  }

  case class BooleanColumn(col: Column)

  object BooleanColumn extends StaticColumnOps {

    override type T  = BooleanColumn
    override type BT = Boolean

    override def sparkType: DataType = BooleanType

    override def createCol(column: Column): BooleanColumn = BooleanColumn(column)

    override def getCol(tc: BooleanColumn): Column = tc.col
  }

  case class StringColumn(col: Column)

  object StringColumn extends StaticColumnOps {

    override type T  = StringColumn
    override type BT = String

    override def sparkType: DataType = StringType

    override def createCol(column: Column): StringColumn = StringColumn(column)

    override def getCol(tc: StringColumn): Column = tc.col
  }

}
