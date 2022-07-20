package doric
package types

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import shapeless._, shapeless.labelled._
import cats.data.{Kleisli, Validated}
import cats.implicits._
import doric.sem.{ColumnTypeError, Location, SparkErrorWrapper}
import org.apache.spark.sql.catalyst.ScalaReflection

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.types._

/**
  * Typeclass to relate a type T with it's spark DataType
  *
  * @tparam T
  * the scala type of the instance
  */
@implicitNotFound(
  "Cant use the type ${T} to generate the typed column. Check your imported SparkType[${T}] instances"
)
sealed trait SparkType[T] {
  self =>

  /**
    * The spark DataType
    *
    * @return
    * the spark DataType
    */
  def dataType: DataType

  /**
    * Scala type when transformed to the dataType
    */
  type OriginalSparkType

  /**
    * Validates if a column of the in put dataframe exist and is of the spark
    * DataType provided
    *
    * @param colName
    * name of the column to extract
    * @param location
    * object that links to the position if an error is created
    * @return
    * A Doric column that validates al the logic
    */
  def validate(colName: String)(implicit location: Location): Doric[Column] = {
    Kleisli[DoricValidated, Dataset[_], Column](df => {
      try {
        val column = df(colName)
        if (isEqual(column.expr.dataType))
          Validated.valid(column)
        else
          ColumnTypeError(colName, dataType, column.expr.dataType).invalidNec
      } catch {
        case e: Throwable => SparkErrorWrapper(e).invalidNec
      }
    })
  }

  /**
    * Checks if the datatype corresponds to the provided datatype, but skipping
    * if can be null
    *
    * @param column
    * the datatype to check
    * @return
    * true if the datatype is equal to the one of the typeclass
    */
  def isEqual(column: DataType): Boolean = column == dataType

  val transform: OriginalSparkType => T

  val rowFieldTransform: Any => OriginalSparkType

  lazy val rowTransformT: Any => T = rowFieldTransform andThen transform

  def customType[O](
      f2: T => O
  ): SparkType.Custom[O, self.OriginalSparkType] = {
    new SparkType[O] {
      override def dataType: DataType = self.dataType

      type OriginalSparkType = self.OriginalSparkType

      override val transform: OriginalSparkType => O = self.transform andThen f2
      override val rowFieldTransform: Any => self.OriginalSparkType =
        self.rowFieldTransform
    }
  }
}

object SparkType {

  type Primitive[T] = SparkType[T] {
    type OriginalSparkType = T
  }

  type Custom[T, O] = SparkType[T] {
    type OriginalSparkType = O
  }

  @inline def apply[T](implicit
      st: SparkType[T]
  ): Custom[T, st.OriginalSparkType] = st

  @inline private def apply[A](dt: DataType): Primitive[A] =
    new SparkType[A] {

      /**
        * The spark DataType
        *
        * @return
        * the spark DataType
        */
      override def dataType: DataType = dt

      override type OriginalSparkType = A

      override val transform: OriginalSparkType => A = identity
      override val rowFieldTransform: Any => A       = _.asInstanceOf[A]
    }

  implicit val fromNull: Primitive[Null] = SparkType[Null](NullType)

  implicit val fromBoolean: Primitive[Boolean] = SparkType[Boolean](BooleanType)

  implicit val fromStringDf: Primitive[String] = SparkType[String](StringType)

  implicit val fromLocalDate: Primitive[Date] =
    SparkType[Date](org.apache.spark.sql.types.DateType)

  implicit val fromInstant: Primitive[Timestamp] =
    SparkType[Timestamp](org.apache.spark.sql.types.TimestampType)

  implicit val fromByte: Primitive[Byte] = SparkType[Byte](ByteType)

  implicit val fromDate: Custom[LocalDate, Date] =
    SparkType[Date].customType[LocalDate](_.toLocalDate)

  implicit val fromTimestamp: Custom[Instant, Timestamp] =
    SparkType[Timestamp].customType[Instant](_.toInstant)

  implicit val fromInt: Primitive[Int] = SparkType[Int](IntegerType)

  implicit val fromLong: Primitive[Long] = SparkType[Long](LongType)

  implicit val fromFloat: Primitive[Float] = SparkType[Float](FloatType)

  implicit val fromBinary: Primitive[Array[Byte]] =
    SparkType[Array[Byte]](org.apache.spark.sql.types.BinaryType)

  implicit val fromDouble: Primitive[Double] = SparkType[Double](DoubleType)

  implicit val fromRow: Primitive[Row] = new SparkType[Row] {
    override def dataType: DataType = StructType(Seq.empty)

    override type OriginalSparkType = Row

    override def isEqual(column: DataType): Boolean = column match {
      case StructType(_) => true
      case _             => false
    }

    override val transform: OriginalSparkType => Row = identity
    override val rowFieldTransform: Any => Row       = _.asInstanceOf[Row]
  }

  implicit val fromHNil: Custom[HNil, Row] = new SparkType[HNil] {
    override type OriginalSparkType = Row
    override val dataType: DataType            = StructType(Seq())
    override val transform: Row => HNil        = _ => HNil
    override val rowFieldTransform: Any => Row = _.asInstanceOf[Row]
  }

  implicit def fromHCons[V, K <: Symbol, VO, T <: HList](implicit
      W: Witness.Aux[K],
      ST: SparkType.Custom[V, VO],
      TS: SparkType.Custom[T, Row]
  ): Custom[FieldType[K, V] :: T, Row] = new SparkType[FieldType[K, V] :: T] {
    override def dataType: DataType =
      StructType(
        StructField(W.value.name, ST.dataType, false) +: TS.dataType
          .asInstanceOf[StructType]
          .fields
      )
    override type OriginalSparkType = Row
    override val transform: Row => FieldType[K, V] :: T = row =>
      field[K](ST.transform(row.getAs[VO](W.value.name))) :: TS.transform(row)
    override val rowFieldTransform: Any => OriginalSparkType =
      _.asInstanceOf[Row]
  }

  implicit def fromProduct[A <: Product: TypeTag, L <: HList](implicit
      lg: LabelledGeneric.Aux[A, L],
      hlistST: SparkType.Custom[L, Row]
  ): SparkType.Custom[A, Row] =
    new SparkType[A] {
      override type OriginalSparkType = Row
      override val dataType            = ScalaReflection.schemaFor[A].dataType
      override val transform: Row => A = hlistST.transform andThen lg.from
      override val rowFieldTransform: Any => Row = _.asInstanceOf[Row]
    }

  implicit def fromMap[K: SparkType, V: SparkType](implicit
      stk: SparkType[K],
      stv: SparkType[V]
  ): Custom[Map[K, V], Map[stk.OriginalSparkType, stv.OriginalSparkType]] =
    new SparkType[Map[K, V]] {
      override def dataType: DataType =
        MapType(stk.dataType, stv.dataType)

      override type OriginalSparkType =
        Map[stk.OriginalSparkType, stv.OriginalSparkType]

      override def isEqual(column: DataType): Boolean = column match {
        case MapType(keyType, valueType, _) =>
          stk.isEqual(keyType) && stv.isEqual(valueType)
        case _ => false
      }

      override val transform: OriginalSparkType => Map[K, V] =
        _.iterator
          .map(x => (stk.transform(x._1), stv.transform(x._2)))
          .toMap

      override val rowFieldTransform
          : Any => Map[stk.OriginalSparkType, stv.OriginalSparkType] =
        _.asInstanceOf[Map[Any, Any]]
          .map(x => (stk.rowFieldTransform(x._1), stv.rowFieldTransform(x._2)))
          .toMap
    }

  implicit def fromArray[A: ClassTag, O: ClassTag](implicit
      st: SparkType[A] { type OriginalSparkType = O }
  ): Custom[Array[A], Array[O]] =
    new SparkType[Array[A]] {
      override def dataType: DataType = ArrayType(
        st.dataType,
        false
      )

      override type OriginalSparkType = Array[st.OriginalSparkType]

      override def isEqual(column: DataType): Boolean = column match {
        case ArrayType(left, _) => st.isEqual(left)
        case _                  => false
      }

      override val transform: OriginalSparkType => Array[A] =
        _.iterator
          .map(st.transform)
          .toArray

      override val rowFieldTransform: Any => Array[O] =
        _.asInstanceOf[DoricArray.Collection[O]].toArray

    }

  implicit def fromList[A](implicit
      st: SparkType[A]
  ): Custom[List[A], List[st.OriginalSparkType]] =
    new SparkType[List[A]] {
      override def dataType: DataType = ArrayType(
        SparkType[A].dataType,
        false
      )

      override type OriginalSparkType = List[st.OriginalSparkType]

      override def isEqual(column: DataType): Boolean = column match {
        case ArrayType(left, _) => st.isEqual(left)
        case _                  => false
      }

      override val transform: OriginalSparkType => List[A] =
        _.map(st.transform)

      override val rowFieldTransform: Any => OriginalSparkType =
        _.asInstanceOf[DoricArray.Collection[st.OriginalSparkType]]
          .map(st.rowFieldTransform)
          .toList
    }

  implicit def fromOption[A](implicit
      st: SparkType[A]
  ): Custom[Option[A], st.OriginalSparkType] =
    new SparkType[Option[A]] {

      override def isEqual(column: DataType): Boolean = st.isEqual(column)

      override def dataType: DataType = st.dataType

      override type OriginalSparkType = st.OriginalSparkType

      override val transform: OriginalSparkType => Option[A] = {
        case null => None
        case x    => Some(st.transform(x))
      }

      override val rowFieldTransform: Any => st.OriginalSparkType = {
        case null => null.asInstanceOf[OriginalSparkType]
        case x    => st.rowFieldTransform(x)
      }
    }
}
