package habla

import cats.data.{Kleisli, ValidatedNec}
import cats.implicits._
import cats.Applicative
import habla.doric.syntax._
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types._

package object doric
    extends FromDfExtras
    with DataFrameOps
    with NumericOperationsOps
    with LiteralConversions
    with CommonColumnOps
    with TimestampColumnLikeOps
    with DateColumnLikeOps {

  type DoricValidated[T] = ValidatedNec[DoricSingleError, T]
  type Doric[T]          = Kleisli[DoricValidated, DataFrame, T]

  implicit val timestampDateOps: DateColumnLike[Timestamp] =
    new DateColumnLike[Timestamp] {}

  implicit class DoricColumnops(elem: Doric[Column]) {
    def toDC[A]: DoricColumn[A] = DoricColumn(elem)
  }

  type TimestampColumn = DoricColumn[Timestamp]

  implicit val timestampOps: TimestampColumnLike[Timestamp] =
    new TimestampColumnLike[Timestamp] {}

  type InstantColumn = DoricColumn[Instant]

  implicit val instantOps: TimestampColumnLike[Instant] =
    new TimestampColumnLike[Instant] {}

  implicit val floatArith: NumericOperations[Float] =
    new NumericOperations[Float] {}

  implicit val fromTimestamp: FromDf[Timestamp] = new FromDf[Timestamp] {

    override def dataType: DataType = TimestampType

  }

  implicit val fromInstant: FromDf[Instant] = new FromDf[Instant] {

    override def dataType: DataType = TimestampType

  }

  implicit val floatCastToString: Casting[Float, String] =
    new SparkCasting[Float, String] {}
  implicit val floatCastToDouble: Casting[Float, Double] =
    new SparkCasting[Float, Double] {}

  type DateColumn = DoricColumn[Date]

  object DateColumn {

    def unapply(column: Column): Option[DateColumn] =
      DoricColumnExtr.unapply[Date](column)

  }

  implicit val fromDate: FromDf[Date] = new FromDf[Date] {
    override val dataType: DataType = DateType
  }

  implicit val fromLocalDate: FromDf[LocalDate] = new FromDf[LocalDate] {
    override val dataType: DataType = DateType
  }

  implicit val dateCol: DateColumnLike[Date] = new DateColumnLike[Date] {}

  implicit val localdateOps: DateColumnLike[Instant] =
    new DateColumnLike[Instant] {}

  type IntegerColumn = DoricColumn[Int]

  object IntegerColumn {

    def apply(litv: Int): IntegerColumn =
      litv.lit

    def unapply(column: Column): Option[IntegerColumn] =
      DoricColumnExtr.unapply[Int](column)
  }
  implicit val fromInt: FromDf[Int] = new FromDf[Int] {

    override def dataType: DataType = IntegerType

  }

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

  case class DoricColumn[T](elem: Doric[Column])

  implicit val fromLong: FromDf[Long] = new FromDf[Long] {

    override def dataType: DataType = LongType

  }

  implicit val longArith: NumericOperations[Long] =
    new NumericOperations[Long] {}

  implicit val longCastToString: Casting[Long, String] =
    new SparkCasting[Long, String] {}

  implicit val longCastToFloat: Casting[Long, Float] =
    new SparkCasting[Long, Float] {}

  implicit val longCastToDouble: Casting[Long, Double] =
    new SparkCasting[Long, Double] {}

  type FloatColumn = DoricColumn[Float]

  object FloatColumn {

    def apply(litv: Float): FloatColumn =
      litv.lit

    def unapply(column: Column): Option[FloatColumn] =
      DoricColumnExtr.unapply[Float](column)
  }

  implicit val fromFloat: FromDf[Float] = new FromDf[Float] {

    override def dataType: DataType = FloatType
  }

  object DoricColumn {
    def apply[T](f: DataFrame => DoricValidated[Column]): DoricColumn[T] =
      DoricColumn(Kleisli(f))

    def apply[T](col: Column): DoricColumn[T] = {
      Kleisli[DoricValidated, DataFrame, Column]((_: DataFrame) => col.valid)
    }.toDC
  }

  object DoricColumnExtr {
    def unapply[A: FromDf](
        column: Column
    )(implicit ap: Applicative[Doric]): Option[DoricColumn[A]] = {
      if (FromDf[A].isValid(column.expr.dataType))
        Some(column.pure[Doric].toDC)
      else
        None
    }
  }

  object TimestampColumn {

    def unapply(column: Column): Option[TimestampColumn] =
      DoricColumnExtr.unapply[Timestamp](column)
  }

  object LongColumn {

    def apply(litv: Long): LongColumn =
      litv.lit

    def unapply(column: Column): Option[LongColumn] =
      DoricColumnExtr.unapply[Long](column)

  }

  type DoubleColumn = DoricColumn[Double]

  object DoubleColumn {

    def apply(litv: Double): DoubleColumn =
      litv.lit

    def unapply(column: Column): Option[DoubleColumn] =
      DoricColumnExtr.unapply[Double](column)

  }

  implicit val fromDouble: FromDf[Double] = new FromDf[Double] {

    override def dataType: DataType = DoubleType
  }

  implicit val doubleArith: NumericOperations[DoubleColumn] =
    new NumericOperations[DoubleColumn] {}

  implicit val doubleCastToString: Casting[DoubleColumn, StringColumn] =
    new SparkCasting[DoubleColumn, StringColumn] {}

  type BooleanColumn = DoricColumn[Boolean]

  object BooleanColumn {

    def apply(litv: Boolean): BooleanColumn =
      litv.lit

    def unapply(column: Column): Option[BooleanColumn] =
      DoricColumnExtr.unapply[Boolean](column)
  }

  implicit val fromBoolean: FromDf[Boolean] = new FromDf[Boolean] {

    override def dataType: DataType = BooleanType
  }

  type StringColumn = DoricColumn[String]

  object StringColumn {

    def apply(litv: String): StringColumn =
      litv.lit

    def unapply(column: Column): Option[StringColumn] =
      DoricColumnExtr.unapply[String](column)
  }

  implicit val fromStringDf: FromDf[String] = new FromDf[String] {

    override def dataType: DataType = StringType
  }

  implicit val stringCastToInt: UnsafeCasting[String, Int] =
    new SparkUnsafeCasting[String, Int] {}

  implicit val stringCastToLong: UnsafeCasting[String, Long] =
    new SparkUnsafeCasting[String, Long] {}

  type ArrayColumn[A] = DoricColumn[Array[A]]

  object ArrayColumn {
    def apply[A](litv: Array[A]): ArrayColumn[A] =
      litv.lit
  }

  implicit def fromArray[A: FromDf]: FromDf[Array[A]] = new FromDf[Array[A]] {
    override def dataType: DataType = ArrayType(implicitly[FromDf[A]].dataType)

    override def isValid(column: DataType): Boolean = column match {
      case ArrayType(left, _) => FromDf[A].isValid(left)
      case _                  => false
    }
  }

  type MapColumn[K, V] = DoricColumn[Map[K, V]]

  implicit def fromMap[K: FromDf, V: FromDf]: FromDf[Map[K, V]] =
    new FromDf[Map[K, V]] {
      override def dataType: DataType =
        MapType(FromDf[K].dataType, FromDf[V].dataType)

      override def isValid(column: DataType): Boolean = column match {
        case MapType(keyType, valueType, _) =>
          FromDf[K].isValid(keyType) && FromDf[V].isValid(valueType)
        case _ => false
      }
    }

  sealed trait DStruct

  object DStruct extends DStruct {}

  type DStructColumn = DoricColumn[DStruct]

  implicit val fromDStruct: FromDf[DStruct] = new FromDf[DStruct] {
    override def dataType: DataType = StructType(Seq.empty)

    override def isValid(column: DataType): Boolean = column match {
      case StructType(_) => true
      case _             => false
    }
  }

}
