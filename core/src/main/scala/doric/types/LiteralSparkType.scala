package doric
package types

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

trait LiteralSparkType[T] {
  self =>
  type OriginalSparkType

  val literalTo: T => OriginalSparkType

  def customType[O](
      f: O => T
  )(implicit ost: SparkType[O]): LiteralSparkType[O] {
    type OriginalSparkType = LiteralSparkType.this.OriginalSparkType
  } =
    new LiteralSparkType[O]() {
      override type OriginalSparkType = self.OriginalSparkType
      override val literalTo: O => OriginalSparkType = f andThen self.literalTo
    }
}

object LiteralSparkType {

  @inline def apply[T](implicit
      litc: LiteralSparkType[T]
  ): LiteralSparkType[T] { type OriginalSparkType = litc.OriginalSparkType } =
    litc

  @inline private def createBasic[T](implicit
      spark: SparkType[T] { type OriginalSparkType = T }
  ): LiteralSparkType[T] {
    type OriginalSparkType = spark.OriginalSparkType
  } = new LiteralSparkType[T] {
    override type OriginalSparkType = spark.OriginalSparkType
    override val literalTo: T => OriginalSparkType = identity
  }

  implicit val fromNull: LiteralSparkType[Null] {
    type OriginalSparkType = Null
  } = createBasic[Null]

  implicit val fromBoolean: LiteralSparkType[Boolean] {
    type OriginalSparkType = Boolean
  } = createBasic[Boolean]

  implicit val fromStringDf: LiteralSparkType[String] {
    type OriginalSparkType = String
  } = createBasic[String]

  implicit val fromLocalDate: LiteralSparkType[LocalDate] {
    type OriginalSparkType = LocalDate
  } =
    createBasic[LocalDate]

  implicit val fromInstant: LiteralSparkType[Instant] {
    type OriginalSparkType = Instant
  } =
    createBasic[Instant]

  implicit val fromDate: LiteralSparkType[Date] {
    type OriginalSparkType = LocalDate
  } =
    LiteralSparkType[LocalDate].customType[Date](_.toLocalDate)

  implicit val fromTimestamp: LiteralSparkType[Timestamp] {
    type OriginalSparkType = Instant
  } =
    LiteralSparkType[Instant].customType[Timestamp](_.toInstant)

  implicit val fromInt: LiteralSparkType[Int] {
    type OriginalSparkType = Int
  } = createBasic[Int]

  implicit val fromLong: LiteralSparkType[Long] {
    type OriginalSparkType = Long
  } = createBasic[Long]

  implicit val fromFloat: LiteralSparkType[Float] {
    type OriginalSparkType = Float
  } = createBasic[Float]

  implicit val fromDouble: LiteralSparkType[Double] {
    type OriginalSparkType = Double
  } = createBasic[Double]

  implicit def fromMap[
      K: LiteralSparkType: SparkType,
      V: LiteralSparkType: SparkType
  ](implicit
      stk: LiteralSparkType[K],
      stv: LiteralSparkType[V]
  ): LiteralSparkType[Map[K, V]] {
    type OriginalSparkType =
      Map[stk.OriginalSparkType, stv.OriginalSparkType]
  } =
    new LiteralSparkType[Map[K, V]] {

      override type OriginalSparkType =
        Map[stk.OriginalSparkType, stv.OriginalSparkType]

      override val literalTo: Map[K, V] => OriginalSparkType =
        _.map(x => (stk.literalTo(x._1), stv.literalTo(x._2))).toMap
    }

  implicit def fromList[A](implicit
      lst: LiteralSparkType[A]
  ): LiteralSparkType[List[A]] {
    type OriginalSparkType = List[lst.OriginalSparkType]
  } =
    new LiteralSparkType[List[A]] {

      override type OriginalSparkType = List[lst.OriginalSparkType]

      override val literalTo: List[A] => OriginalSparkType =
        _.map(lst.literalTo)

    }

  implicit def fromOption[A](implicit
      lst: LiteralSparkType[A]
  ): LiteralSparkType[Option[A]] {
    type OriginalSparkType = lst.OriginalSparkType
  } =
    new LiteralSparkType[Option[A]] {

      override type OriginalSparkType = lst.OriginalSparkType

      override val literalTo: Option[A] => OriginalSparkType = {
        case Some(x) => lst.literalTo(x)
        case None    => null.asInstanceOf[OriginalSparkType]
      }
    }
}
