package habla

import habla.doric.syntax.{
  CommonColumnOps,
  DataFrameOps,
  FromDfExtras,
  LiteralConversions,
  NumericOperations,
  NumericOperationsOps,
  ToColumnExtras,
  TimestampColumnLike,
  TimestampColumnLikeOps,
  DateColumnLike,
  DateColumnLikeOps
}

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, functions}

import java.sql.{Date, Timestamp}

package object doric
    extends ToColumnExtras
    with FromDfExtras
    with DataFrameOps
    with NumericOperationsOps
    with LiteralConversions
    with CommonColumnOps
    with TimestampColumnLikeOps
    with DateColumnLikeOps {

  case class TimestampColumn(col: Column)

  object TimestampColumn {

    def unapply(column: Column): Option[TimestampColumn] = {
      if (column.expr.dataType == TimestampType)
        Some(TimestampColumn(column))
      else
        None
    }
    implicit val fromDf: FromDf[TimestampColumn] = new FromDf[TimestampColumn] {

      override def dataType: DataType = TimestampType

      override val construct: Column => TimestampColumn = TimestampColumn.apply

      override val column: TimestampColumn => Column = _.col
    }

    implicit val literal: Literal[TimestampColumn, Timestamp] =
      new Literal[TimestampColumn, Timestamp] {}

    implicit val timestampOps = new TimestampColumnLike[TimestampColumn] {}
    implicit val timestampDateOps = new DateColumnLike[TimestampColumn] {}

  }

  case class DateColumn(col: Column)

  object DateColumn {

    def unapply(column: Column): Option[DateColumn] = {
      if (column.expr.dataType == DateType)
        Some(DateColumn(column))
      else
        None
    }
    implicit val fromDf: FromDf[DateColumn] = new FromDf[DateColumn] {

      override def dataType: DataType = DateType

      override val construct: Column => DateColumn = DateColumn.apply

      override val column: DateColumn => Column = _.col
    }

    implicit val literal: Literal[DateColumn, Date] =
      new Literal[DateColumn, Date] {}

    implicit val dateCol: DateColumnLike[DateColumn] = new DateColumnLike[DateColumn] {}
  }

  case class IntegerColumn(col: Column)

  object IntegerColumn {

    type Lit[T] = Literal[IntegerColumn, T]

    def apply[LT: Lit](lit: LT): IntegerColumn =
      implicitly[Lit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[IntegerColumn] = {
      if (column.expr.dataType == IntegerType)
        Some(IntegerColumn(column))
      else
        None
    }
    implicit val fromDf: FromDf[IntegerColumn] = new FromDf[IntegerColumn] {

      override def dataType: DataType = IntegerType

      override val construct: Column => IntegerColumn = IntegerColumn.apply

      override val column: IntegerColumn => Column = _.col
    }

    implicit val literal: Literal[IntegerColumn, Int] =
      new Literal[IntegerColumn, Int] {}

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

  object LongColumn {

    type Lit[T] = Literal[LongColumn, T]

    def apply[LT: Lit](lit: LT): LongColumn =
      implicitly[Lit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[LongColumn] = {
      if (column.expr.dataType == LongType)
        Some(LongColumn(column))
      else
        None
    }

    implicit val fromDf: FromDf[LongColumn] = new FromDf[LongColumn] {

      override def dataType: DataType = LongType

      override val construct: Column => LongColumn = LongColumn.apply

      override val column: LongColumn => Column = _.col
    }

    implicit val literal: Literal[LongColumn, Int] =
      new Literal[LongColumn, Int] {}

    implicit val intArith: NumericOperations[LongColumn] = NumericOperations[LongColumn]()

    implicit val castToString: Casting[LongColumn, StringColumn] =
      new SparkCasting[LongColumn, StringColumn] {}

    implicit val castToFloat: Casting[LongColumn, FloatColumn] =
      new SparkCasting[LongColumn, FloatColumn] {}

    implicit val castToDouble: Casting[LongColumn, DoubleColumn] =
      new SparkCasting[LongColumn, DoubleColumn] {}

  }

  case class FloatColumn(col: Column)

  object FloatColumn {

    type Lit[T] = Literal[FloatColumn, T]

    def apply[LT: Lit](lit: LT): FloatColumn =
      implicitly[Lit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[FloatColumn] = {
      if (column.expr.dataType == FloatType)
        Some(FloatColumn(column))
      else
        None
    }

    implicit val fromDf: FromDf[FloatColumn] = new FromDf[FloatColumn] {

      override def dataType: DataType = FloatType

      override val construct: Column => FloatColumn = FloatColumn.apply

      override val column: FloatColumn => Column = _.col
    }

    implicit val literal: Literal[FloatColumn, Float] =
      new Literal[FloatColumn, Float] {}

    implicit val intArith: NumericOperations[FloatColumn] = NumericOperations[FloatColumn]()

    implicit val castToString: Casting[FloatColumn, StringColumn] =
      new SparkCasting[FloatColumn, StringColumn] {}

    implicit val castToDouble: Casting[FloatColumn, DoubleColumn] =
      new SparkCasting[FloatColumn, DoubleColumn] {}

  }

  case class DoubleColumn(col: Column)

  object DoubleColumn {

    type Lit[T] = Literal[DoubleColumn, T]

    def apply[LT: Lit](lit: LT): DoubleColumn =
      implicitly[Lit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[DoubleColumn] = {
      if (column.expr.dataType == DoubleType)
        Some(DoubleColumn(column))
      else
        None
    }

    implicit val fromDf: FromDf[DoubleColumn] = new FromDf[DoubleColumn] {

      override def dataType: DataType = DoubleType

      override val construct: Column => DoubleColumn = DoubleColumn.apply

      override val column: DoubleColumn => Column = _.col
    }

    implicit val literal: Literal[DoubleColumn, Double] =
      new Literal[DoubleColumn, Double] {}

    implicit val intArith: NumericOperations[DoubleColumn] = NumericOperations[DoubleColumn]()

    implicit val castToString: Casting[DoubleColumn, StringColumn] =
      new SparkCasting[DoubleColumn, StringColumn] {}

  }

  case class BooleanColumn(col: Column)

  object BooleanColumn {

    type Lit[T] = Literal[BooleanColumn, T]

    def apply[LT: Lit](lit: LT): BooleanColumn =
      implicitly[Lit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[BooleanColumn] = {
      if (column.expr.dataType == BooleanType)
        Some(BooleanColumn(column))
      else
        None
    }

    implicit val fromDf: FromDf[BooleanColumn] = new FromDf[BooleanColumn] {

      override def dataType: DataType = BooleanType

      override val construct: Column => BooleanColumn = BooleanColumn.apply

      override val column: BooleanColumn => Column = _.col
    }

    implicit val literal: Literal[BooleanColumn, Boolean] =
      new Literal[BooleanColumn, Boolean] {}
  }

  case class StringColumn(col: Column)

  object StringColumn {

    type Lit[T] = Literal[StringColumn, T]

    def apply[LT: Lit](lit: LT): StringColumn =
      implicitly[Lit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[StringColumn] = {
      if (column.expr.dataType == StringType)
        Some(StringColumn(column))
      else
        None
    }

    implicit val fromDf: FromDf[StringColumn] = new FromDf[StringColumn] {

      override def dataType: DataType = StringType

      override val construct: Column => StringColumn = StringColumn.apply

      override val column: StringColumn => Column = _.col
    }

    implicit val literal: Literal[StringColumn, String] =
      new Literal[StringColumn, String] {}
  }

}
