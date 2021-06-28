package doric
package types

import scala.annotation.implicitNotFound

import cats.data.{Kleisli, Validated}
import cats.implicits._
import doric.sem.{ColumnTypeError, Location, SparkErrorWrapper}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.types._

/**
  * Typeclass to relate a type T with it's spark DataType
  * @tparam T the scala type of the instance
  */
@implicitNotFound(
  "Cant use the type ${T} to generate the typed column. Check your imported SparkType[${T}] instances"
)
trait SparkType[T] {

  /**
    * The spark DataType
    * @return the spark DataType
    */
  def dataType: DataType

  /**
    * Validates if a column of the in put dataframe exist and is of the spark DataType provided
    * @param colName name of the column to extract
    * @param location object that links to the position if an error is created
    * @return A Doric column that validates al the logic
    */
  def validate(colName: String)(implicit location: Location): Doric[Column] = {
    Kleisli[DoricValidated, Dataset[_], Column](df => {
      try {
        val column = df(colName)
        if (isValid(column.expr.dataType))
          Validated.valid(column)
        else
          ColumnTypeError(colName, dataType, column.expr.dataType).invalidNec
      } catch {
        case e: Throwable => SparkErrorWrapper(e).invalidNec
      }
    })
  }

  /**
    * Checks if the datatype corresponds to the provided datatype, but skipping if can be null
    * @param column the datatype to check
    * @return true if the datatype is equal to the one of the typeclass
    */
  def isValid(column: DataType): Boolean = column == dataType

}

object SparkType {
  @inline def apply[A: SparkType]: SparkType[A] = implicitly[SparkType[A]]

  def apply[A](dt: DataType): SparkType[A] = new SparkType[A] {

    /**
      * The spark DataType
      *
      * @return the spark DataType
      */
    override def dataType: DataType = dt
  }

  implicit val fromBoolean: SparkType[Boolean] = SparkType[Boolean](BooleanType)

  implicit val fromStringDf: SparkType[String] = SparkType[String](StringType)

  implicit val fromDate: SparkType[Date] =
    SparkType[Date](org.apache.spark.sql.types.DateType)

  implicit val fromLocalDate: SparkType[LocalDate] =
    SparkType[LocalDate](org.apache.spark.sql.types.DateType)

  implicit val fromTimestamp: SparkType[Timestamp] =
    SparkType[Timestamp](org.apache.spark.sql.types.TimestampType)

  implicit val fromInstant: SparkType[Instant] =
    SparkType[Instant](org.apache.spark.sql.types.TimestampType)

  implicit val fromInt: SparkType[Int] = SparkType[Int](IntegerType)

  implicit val fromLong: SparkType[Long] = SparkType[Long](LongType)

  implicit val fromFloat: SparkType[Float] = SparkType[Float](FloatType)

  implicit val fromDouble: SparkType[Double] = SparkType[Double](DoubleType)

  implicit val fromDStruct: SparkType[DStruct] = new SparkType[DStruct] {
    override def dataType: DataType = StructType(Seq.empty)

    override def isValid(column: DataType): Boolean = column match {
      case StructType(_) => true
      case _             => false
    }
  }

  implicit def fromMap[K: SparkType, V: SparkType]: SparkType[Map[K, V]] =
    new SparkType[Map[K, V]] {
      override def dataType: DataType =
        MapType(SparkType[K].dataType, SparkType[V].dataType)

      override def isValid(column: DataType): Boolean = column match {
        case MapType(keyType, valueType, _) =>
          SparkType[K].isValid(keyType) && SparkType[V].isValid(valueType)
        case _ => false
      }
    }

  implicit def fromArray[A: SparkType]: SparkType[Array[A]] =
    new SparkType[Array[A]] {
      override def dataType: DataType = ArrayType(
        implicitly[SparkType[A]].dataType
      )

      override def isValid(column: DataType): Boolean = column match {
        case ArrayType(left, _) => SparkType[A].isValid(left)
        case _                  => false
      }
    }
}
