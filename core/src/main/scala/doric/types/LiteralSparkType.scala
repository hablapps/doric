package doric
package types

import cats.implicits.catsSyntaxValidatedIdBinCompat0
import doric.sem.SparkErrorWrapper
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.{Column, Row, functions => f}

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import scala.reflect.runtime.universe

trait LiteralSparkType[T] {
  self =>
  type OriginalSparkType

  val classTag: ClassTag[OriginalSparkType]

  val ttag: TypeTag[OriginalSparkType]

  val literalTo: T => OriginalSparkType

  def literal(t: T): DoricValidated[Column] =
    f.typedlit[OriginalSparkType](literalTo(t))(ttag).validNec

  def customType[O](
      f: O => T
  )(implicit
      ost: SparkType[O]
  ): LiteralSparkType.Custom[O, self.OriginalSparkType] =
    new LiteralSparkType[O]() {
      override type OriginalSparkType = self.OriginalSparkType
      val ttag                                       = self.ttag
      val classTag: ClassTag[self.OriginalSparkType] = self.classTag
      override val literalTo: O => OriginalSparkType = f andThen self.literalTo
    }
}

object LiteralSparkType extends LiteralSparkTypeLPI {

  type Primitive[T] = LiteralSparkType[T] {
    type OriginalSparkType = T
  }

  type Custom[T, O] = LiteralSparkType[T] {
    type OriginalSparkType = O
  }

  @inline def apply[T](implicit
      litc: LiteralSparkType[T]
  ): Custom[T, litc.OriginalSparkType] =
    litc

  @inline protected def createPrimitive[T: ClassTag: TypeTag]: Primitive[T] =
    new LiteralSparkType[T] {
      override type OriginalSparkType = T
      val classTag: ClassTag[T]                      = implicitly[ClassTag[T]]
      val ttag                                       = typeTag[T]
      override val literalTo: T => OriginalSparkType = identity
    }

  implicit val fromNull: Primitive[Null] = createPrimitive[Null]

  implicit val fromBoolean: Primitive[Boolean] = createPrimitive[Boolean]

  implicit val fromStringDf: Primitive[String] = createPrimitive[String]

  implicit val fromLocalDate: Primitive[Date] =
    createPrimitive[Date]

  implicit val fromInstant: Primitive[Timestamp] =
    createPrimitive[Timestamp]

  implicit val fromDate: Custom[LocalDate, Date] =
    LiteralSparkType[Date].customType[LocalDate](Date.valueOf)

  implicit val fromTimestamp: Custom[Instant, Timestamp] =
    LiteralSparkType[Timestamp].customType[Instant](Timestamp.from)

  implicit val fromInt: Primitive[Int] = createPrimitive[Int]

  implicit val fromLong: Primitive[Long] = createPrimitive[Long]

  implicit val fromFloat: Primitive[Float] = createPrimitive[Float]

  implicit val fromDouble: Primitive[Double] = createPrimitive[Double]

  implicit val fromRow: Primitive[Row] = new LiteralSparkType[Row] {
    override type OriginalSparkType = Row
    override val classTag: ClassTag[Row]     = implicitly[ClassTag[Row]]
    override val ttag: universe.TypeTag[Row] = typeTag[Row]
    override val literalTo                   = identity
    override def literal(t: Row): DoricValidated[Column] =
      if (t.schema == null)
        SparkErrorWrapper(new Exception("Row without schema")).invalidNec
      else
        new Column(
          Literal(
            CatalystTypeConverters.createToCatalystConverter(t.schema)(t),
            t.schema
          )
        ).validNec
  }

  implicit def fromMap[K, V](implicit
      stk: LiteralSparkType[K],
      stv: LiteralSparkType[V]
  ): Custom[Map[K, V], Map[stk.OriginalSparkType, stv.OriginalSparkType]] =
    new LiteralSparkType[Map[K, V]] {

      override type OriginalSparkType =
        Map[stk.OriginalSparkType, stv.OriginalSparkType]

      val ttag = maptt(stk.ttag, stv.ttag)

      override val literalTo: Map[K, V] => OriginalSparkType =
        _.map(x => (stk.literalTo(x._1), stv.literalTo(x._2))).toMap
      override val classTag
          : ClassTag[Map[stk.OriginalSparkType, stv.OriginalSparkType]] =
        implicitly[ClassTag[Map[stk.OriginalSparkType, stv.OriginalSparkType]]]
    }

  implicit def fromList[A](implicit
      lst: LiteralSparkType[A]
  ): Custom[List[A], List[lst.OriginalSparkType]] =
    new LiteralSparkType[List[A]] {

      override type OriginalSparkType = List[lst.OriginalSparkType]

      override val literalTo: List[A] => OriginalSparkType =
        _.map(lst.literalTo)
      override val classTag: ClassTag[List[lst.OriginalSparkType]] =
        implicitly[ClassTag[List[lst.OriginalSparkType]]]

      val ttag = listtt(lst.ttag)
    }

  implicit def fromArray[A](implicit
      lst: LiteralSparkType[A]
  ): Custom[Array[A], Array[lst.OriginalSparkType]] =
    new LiteralSparkType[Array[A]] {

      override type OriginalSparkType = Array[lst.OriginalSparkType]

      override val literalTo: Array[A] => OriginalSparkType = {
        implicit val a = lst.classTag
        _.map(lst.literalTo)
          .toArray(lst.classTag)
      }

      override val classTag: ClassTag[Array[lst.OriginalSparkType]] =
        lst.classTag.wrap

      val ttag = arraytt(lst.ttag)
    }

  implicit def fromOption[A](implicit
      lst: LiteralSparkType[A]
  ): Custom[Option[A], lst.OriginalSparkType] =
    new LiteralSparkType[Option[A]] {

      override type OriginalSparkType = lst.OriginalSparkType

      override val literalTo: Option[A] => OriginalSparkType = {
        case Some(x) => lst.literalTo(x)
        case None    => null.asInstanceOf[OriginalSparkType]
      }
      override val classTag: ClassTag[lst.OriginalSparkType] =
        lst.classTag

      val ttag = lst.ttag
    }

  def optiontt[T: TypeTag]: TypeTag[Option[T]]          = typeTag[Option[T]]
  def maptt[K: TypeTag, V: TypeTag]: TypeTag[Map[K, V]] = typeTag[Map[K, V]]
  def listtt[T: TypeTag]: TypeTag[List[T]]              = typeTag[List[T]]
  def arraytt[T: TypeTag]: TypeTag[Array[T]]            = typeTag[Array[T]]
}

trait LiteralSparkTypeLPI { self: LiteralSparkType.type =>

  implicit def fromProduct[T <: Product: TypeTag: ClassTag]: Primitive[T] =
    createPrimitive[T]

}
