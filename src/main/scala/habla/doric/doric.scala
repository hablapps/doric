package habla

import habla.doric.syntax.{
  CommonColumnOps,
  DataFrameOps,
  FromDfExtras,
  LiteralConversions,
  NumericOperations,
  NumericOperationsOps,
  TimestampColumnLike,
  TimestampColumnLikeOps,
  DateColumnLike,
  DateColumnLikeOps
}

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, functions}

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.TypedColumn

package object doric
    extends FromDfExtras
    with DataFrameOps
    with NumericOperationsOps
    with LiteralConversions
    with CommonColumnOps
    with TimestampColumnLikeOps
    with DateColumnLikeOps {

  case class DoricColumn[A](col: Column)

  object DoricColumnExtr {
    def unapply[A: FromDf](column: Column): Option[DoricColumn[A]] = {
      if (FromDf[A].isValid(column))
        Some(DoricColumn[A](column))
      else
        None
    }
  }

  type TimestampColumn = DoricColumn[Timestamp]

  object TimestampColumn {

    def unapply(column: Column): Option[TimestampColumn] = DoricColumnExtr.unapply(column)
  }

  implicit val fromTimestamp: FromDf[Timestamp] = new FromDf[Timestamp] {

    override def dataType: DataType = TimestampType

  }

  implicit val literalTimestamp: Literal[Timestamp, Timestamp] =
    new Literal[Timestamp, Timestamp] {}

  implicit val timestampOps     = new TimestampColumnLike[Timestamp] {}
  implicit val timestampDateOps = new DateColumnLike[Timestamp] {}

  type DateColumn = DoricColumn[Date]

  object DateColumn {

    def unapply(column: Column): Option[DateColumn] = DoricColumnExtr.unapply[Date](column)

  }

  implicit val fromDate: FromDf[Date] = new FromDf[Date] {
    override val dataType: DataType = DateType
  }

  implicit val literalDate: Literal[Date, Date] =
    new Literal[Date, Date] {}

  implicit val dateCol: DateColumnLike[Date] = new DateColumnLike[Date] {}

  type IntegerColumn = DoricColumn[Int]

  object IntegerColumn {

    type Lit[T] = Literal[Int, T]

    def apply[LT: Lit](lit: LT): IntegerColumn =
      implicitly[Lit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[IntegerColumn] = DoricColumnExtr.unapply[Int](column)
  }
  implicit val fromInt: FromDf[Int] = new FromDf[Int] {

    override def dataType: DataType = IntegerType

  }

  implicit val literal: Literal[Int, Int] =
    new Literal[Int, Int] {}

  implicit val intArith: NumericOperations[Int] = new NumericOperations[Int] {}

  implicit val intCastToString: Casting[Int, String] =
    new SparkCasting[Int, String] {}

  implicit val intCastToLong: Casting[Int, Long] =
    new SparkCasting[Int, Long] {}

  implicit val intCastToFloat: Casting[Int, Float] =
    new SparkCasting[Int, Float] {}

  implicit val intCastToDouble: Casting[Int, Double] =
    new SparkCasting[Int, Double] {}

  type LongColumn = DoricColumn[Long]

  object LongColumn {

    def apply[LT: LongLit](lit: LT): LongColumn =
      implicitly[LongLit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[LongColumn] = DoricColumnExtr.unapply(column)

  }

  type LongLit[T] = Literal[Long, T]

  implicit val fromLong: FromDf[Long] = new FromDf[Long] {

    override def dataType: DataType = LongType

  }

  implicit val literalLong: Literal[Long, Int] =
    new Literal[Long, Int] {}

  implicit val longArith: NumericOperations[Long] = new NumericOperations[Long] {}

  implicit val longCastToString: Casting[Long, String] =
    new SparkCasting[Long, String] {}

  implicit val longCastToFloat: Casting[Long, Float] =
    new SparkCasting[Long, Float] {}

  implicit val longCastToDouble: Casting[Long, Double] =
    new SparkCasting[Long, Double] {}

  type FloatColumn = DoricColumn[Float]

  object FloatColumn {

    def apply[LT: FloatLit](lit: LT): FloatColumn =
      implicitly[FloatLit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[FloatColumn] = DoricColumnExtr.unapply[Float](column)
  }
  type FloatLit[T] = Literal[Float, T]

  implicit val fromFloat: FromDf[Float] = new FromDf[Float] {

    override def dataType: DataType = FloatType
  }

  implicit val literalFloat: Literal[FloatColumn, Float] =
    new Literal[FloatColumn, Float] {}

  implicit val floatArith: NumericOperations[FloatColumn] = new NumericOperations[FloatColumn] {}

  implicit val floatCastToString: Casting[FloatColumn, StringColumn] =
    new SparkCasting[FloatColumn, StringColumn] {}

  implicit val floatCastToDouble: Casting[FloatColumn, DoubleColumn] =
    new SparkCasting[FloatColumn, DoubleColumn] {}

  type DoubleColumn = DoricColumn[Double]

  object DoubleColumn {

    def apply[LT: DoubleLit](lit: LT): DoubleColumn =
      implicitly[Literal[Double, LT]].createTLiteral(lit)

    def unapply(column: Column): Option[DoubleColumn] = DoricColumnExtr.unapply[Double](column)

  }
  type DoubleLit[T] = Literal[Double, T]
  implicit val fromDouble: FromDf[Double] = new FromDf[Double] {

    override def dataType: DataType = DoubleType
  }

  implicit val literalDouble: Literal[DoubleColumn, Double] =
    new Literal[DoubleColumn, Double] {}

  implicit val doubleArith: NumericOperations[DoubleColumn] = new NumericOperations[DoubleColumn] {}

  implicit val doubleCastToString: Casting[DoubleColumn, StringColumn] =
    new SparkCasting[DoubleColumn, StringColumn] {}

  type BooleanColumn = DoricColumn[Boolean]

  object BooleanColumn {

    def apply[LT: BooleanLit](lit: LT): BooleanColumn =
      implicitly[BooleanLit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[BooleanColumn] = DoricColumnExtr.unapply(column)
  }
  type BooleanLit[T] = Literal[Boolean, T]
  implicit val fromBoolean: FromDf[Boolean] = new FromDf[Boolean] {

    override def dataType: DataType = BooleanType
  }

  implicit val literalBoolean: Literal[BooleanColumn, Boolean] =
    new Literal[BooleanColumn, Boolean] {}

  type StringColumn = DoricColumn[String]

  object StringColumn {

    def apply[LT: StringLit](lit: LT): StringColumn =
      implicitly[StringLit[LT]].createTLiteral(lit)

    def unapply(column: Column): Option[StringColumn] = DoricColumnExtr.unapply(column)
  }

  type StringLit[T] = Literal[String, T]
  implicit val fromStringDf: FromDf[String] = new FromDf[String] {

    override def dataType: DataType = StringType
  }

  implicit val literalString: StringLit[String] =
    new Literal[String, String] {}

  type ArrayColumn[A] = DoricColumn[Array[A]]

  object ArrayColumn {

    type Lit[LT[_], AIT, AITL] = Literal[Array[AIT], LT[AITL]]
    implicit def fromDF[A: FromDf]: FromDf[Array[A]] = new FromDf[Array[A]] {

      override def dataType: DataType = ArrayType(implicitly[FromDf[A]].dataType)

    }

    def unapply[A: FromDf](column: Column): Option[ArrayColumn[A]] = DoricColumnExtr.unapply(column)
  }

  implicit def listLit[IST, A](implicit
      intLit: Literal[A, IST]
  ): Literal[Array[A], Array[IST]] = {
    new Literal[Array[A], Array[IST]] {}
  }

}
