package doric
package types

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

trait Casting[From, To] {

  def cast(column: DoricColumn[From])(implicit
      fromType: SparkType[From],
      toType: SparkType[To]
  ): DoricColumn[To]
}

object Casting {

  @inline def apply[From, To](implicit
      imp: Casting[From, To]
  ): Casting[From, To] =
    implicitly[Casting[From, To]]

  implicit val dateCastToLocalDate: Casting[Date, LocalDate] =
    SparkCasting[Date, LocalDate]

  implicit val localdateCastToLocalDate: Casting[LocalDate, Date] =
    SparkCasting[LocalDate, Date]

  implicit val doubleCastToString: Casting[Double, String] =
    SparkCasting[Double, String]

  implicit val intCastToString: Casting[Int, String] =
    SparkCasting[Int, String]

  implicit val intCastToLong: Casting[Int, Long] =
    SparkCasting[Int, Long]

  implicit val intCastToFloat: Casting[Int, Float] =
    SparkCasting[Int, Float]

  implicit val intCastToDouble: Casting[Int, Double] =
    SparkCasting[Int, Double]

  implicit val longCastToString: Casting[Long, String] =
    SparkCasting[Long, String]

  implicit val longCastToFloat: Casting[Long, Float] = SparkCasting[Long, Float]

  implicit val longCastToDouble: Casting[Long, Double] =
    SparkCasting[Long, Double]

  implicit val floatCastToString: Casting[Float, String] =
    SparkCasting[Float, String]

  implicit val floatCastToDouble: Casting[Float, Double] =
    SparkCasting[Float, Double]

  implicit val timestampCastToInstant: Casting[Timestamp, Instant] =
    SparkCasting[Timestamp, Instant]

  implicit val timestampCastToDate: Casting[Timestamp, Date] =
    SparkCasting[Timestamp, Date]

  implicit val timestampCastToLocalDate: Casting[Timestamp, LocalDate] =
    SparkCasting[Timestamp, LocalDate]
}

trait SparkCasting[From, To] extends Casting[From, To] {

  override def cast(column: DoricColumn[From])(implicit
      fromType: SparkType[From],
      toType: SparkType[To]
  ): DoricColumn[To] =
    if (fromType.dataType == toType.dataType)
      column.elem.toDC
    else
      column.elem.map(_.cast(toType.dataType)).toDC
}

object SparkCasting {
  def apply[From, To]: Casting[From, To] = new SparkCasting[From, To] {}
}
