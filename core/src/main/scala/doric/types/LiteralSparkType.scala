package doric
package types

import cats.implicits.catsSyntaxValidatedIdBinCompat0
import doric.sem.SparkErrorWrapper
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Column, Row, functions => f}
import shapeless.labelled.{FieldType, field}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.reflect.{ClassTag, classTag}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime}
import scala.reflect.runtime.universe

trait LiteralSparkType[T] {
  self =>
  type OriginalSparkType

  val cTag: ClassTag[OriginalSparkType]

  val ttag: TypeTag[OriginalSparkType]

  val literalTo: T => OriginalSparkType

  def literal(t: T): DoricValidated[Column] =
    f.typedLit[OriginalSparkType](literalTo(t))(ttag).validNec

  def customType[O](
      f: O => T
  )(implicit
      ost: SparkType[O]
  ): LiteralSparkType.Custom[O, self.OriginalSparkType] =
    new LiteralSparkType[O]() {
      override type OriginalSparkType = self.OriginalSparkType
      val ttag                                       = self.ttag
      val cTag: ClassTag[self.OriginalSparkType]     = self.cTag
      override val literalTo: O => OriginalSparkType = f andThen self.literalTo
    }
}

object LiteralSparkType extends LiteralSparkTypeLPI_I {

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
      val cTag: ClassTag[T]                          = implicitly[ClassTag[T]]
      val ttag                                       = typeTag[T]
      override val literalTo: T => OriginalSparkType = identity
    }
}

trait LiteralSparkTypeLPI_I extends LiteralSparkTypeLPI_II {
  self: LiteralSparkType.type =>

  implicit val fromNull: Primitive[Null] = createPrimitive[Null]

  implicit val fromInt: Primitive[Int]       = createPrimitive[Int]
  implicit val fromLong: Primitive[Long]     = createPrimitive[Long]
  implicit val fromFloat: Primitive[Float]   = createPrimitive[Float]
  implicit val fromDouble: Primitive[Double] = createPrimitive[Double]
  implicit val fromShort: Primitive[Short]   = createPrimitive[Short]
  implicit val fromByte: Primitive[Byte]     = createPrimitive[Byte]

  implicit val fromJavaInt: Primitive[java.lang.Integer] =
    createPrimitive[java.lang.Integer]
  implicit val fromJavaLong: Primitive[java.lang.Long] =
    createPrimitive[java.lang.Long]
  implicit val fromJavaFloat: Primitive[java.lang.Float] =
    createPrimitive[java.lang.Float]
  implicit val fromJavaDouble: Primitive[java.lang.Double] =
    createPrimitive[java.lang.Double]
  implicit val fromJavaShort: Primitive[java.lang.Short] =
    createPrimitive[java.lang.Short]
  implicit val fromJavaByte: Primitive[java.lang.Byte] =
    createPrimitive[java.lang.Byte]

  // Java numerics: TBD
  // BigDecimal et al.: TBD
  // Binary type: TBD

  implicit val fromBoolean: Primitive[Boolean] = createPrimitive[Boolean]
  implicit val fromJavaBoolean: Primitive[java.lang.Boolean] =
    createPrimitive[java.lang.Boolean]

  implicit val fromStringDf: Primitive[String] = createPrimitive[String]

  implicit def fromDateLST: LiteralSparkType[Date] =
    if (
      SQLConf.get.getConfString(
        "spark.sql.datetime.java8API.enabled",
        "false"
      ) == "false"
    )
      createPrimitive[Date]
    else
      fromLocalDateLST.customType[Date](_.toLocalDate)

  implicit def fromTimestampLST: LiteralSparkType[Timestamp] =
    if (
      SQLConf.get.getConfString(
        "spark.sql.datetime.java8API.enabled",
        "false"
      ) == "false"
    )
      createPrimitive[Timestamp]
    else
      fromInstantLST.customType[Timestamp](_.toInstant)

  implicit def fromLocalDateLST: LiteralSparkType[LocalDate] =
    if (
      SQLConf.get.getConfString(
        "spark.sql.datetime.java8API.enabled",
        "false"
      ) == "true"
    )
      createPrimitive[LocalDate]
    else
      fromDateLST.customType[LocalDate](Date.valueOf)

  implicit def fromInstantLST: LiteralSparkType[Instant] =
    if (
      SQLConf.get.getConfString(
        "spark.sql.datetime.java8API.enabled",
        "false"
      ) == "true"
    )
      createPrimitive[Instant]
    else
      fromTimestampLST.customType[Instant](Timestamp.from)

  // Calendar: TBD

  // Duration: TBD
  // Period: TBD
}

trait LiteralSparkTypeLPI_II extends LiteralSparkTypeLPI_III { // with LiteralSparkTypeLPI_II_Array_Specific{
  self: LiteralSparkType.type =>

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
      override val cTag
          : ClassTag[Map[stk.OriginalSparkType, stv.OriginalSparkType]] =
        implicitly[ClassTag[Map[stk.OriginalSparkType, stv.OriginalSparkType]]]
    }

  implicit def fromArray[A](implicit
      lst: LiteralSparkType[A]
  ): Custom[Array[A], Array[lst.OriginalSparkType]] =
    new LiteralSparkType[Array[A]] {

      override type OriginalSparkType = Array[lst.OriginalSparkType]

      override val literalTo: Array[A] => OriginalSparkType = {
        implicit val lstcTag = lst.cTag // For scala 2.13
        _.map(lst.literalTo)
      }

      override val cTag: ClassTag[Array[lst.OriginalSparkType]] =
        lst.cTag.wrap

      val ttag = arraytt(lst.ttag)
    }

  implicit def fromSeq[A, CC[x] <: Seq[x]](implicit
      lst: LiteralSparkType[A]
  ): Custom[CC[A], Seq[lst.OriginalSparkType]] =
    new LiteralSparkType[CC[A]] {

      override type OriginalSparkType = Seq[lst.OriginalSparkType]

      override val literalTo: CC[A] => OriginalSparkType =
        _.map(lst.literalTo)
      override val cTag: ClassTag[Seq[lst.OriginalSparkType]] =
        implicitly[ClassTag[Seq[lst.OriginalSparkType]]]

      val ttag = seqtt(lst.ttag)
    }

  implicit def fromSet[A, CC[x] <: Set[x]](implicit
      lst: LiteralSparkType[A]
  ): Custom[CC[A], Seq[lst.OriginalSparkType]] =
    new LiteralSparkType[CC[A]] {

      override type OriginalSparkType = Seq[lst.OriginalSparkType]

      override val literalTo: CC[A] => OriginalSparkType =
        _.map(lst.literalTo).toSeq
      override val cTag: ClassTag[Seq[lst.OriginalSparkType]] =
        implicitly[ClassTag[Seq[lst.OriginalSparkType]]]

      val ttag = seqtt(lst.ttag)
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
      override val cTag: ClassTag[lst.OriginalSparkType] =
        lst.cTag

      val ttag = lst.ttag
    }

  def maptt[K: TypeTag, V: TypeTag]: TypeTag[Map[K, V]] = typeTag[Map[K, V]]
  def seqtt[T: TypeTag]: TypeTag[Seq[T]]                = typeTag[Seq[T]]
  def arraytt[T: TypeTag]: TypeTag[Array[T]]            = typeTag[Array[T]]

  implicit val fromRow: Primitive[Row] = new LiteralSparkType[Row] {
    override type OriginalSparkType = Row
    override val cTag: ClassTag[Row]   = implicitly[ClassTag[Row]]
    override val ttag: TypeTag[Row]    = typeTag[Row]
    override val literalTo: Row => Row = identity _
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

}

trait LiteralSparkTypeLPI_III {
  self: LiteralSparkType.type =>

  implicit def fromProduct[T <: Product: TypeTag: ClassTag]: Primitive[T] =
    createPrimitive[T]
}
