package doric
package syntax

import scala.reflect.ClassTag

import doric.sem.Location
import doric.types.SparkType
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.{Column, Dataset, Row}

private[doric] trait ColGetters[F[_]] {
  @inline protected def constructSide[T](
      column: Doric[Column],
      colName: String
  ): F[T]

  /**
    * Retrieves a column with the provided name and the provided type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @tparam T
    * the expected type of the column
    * @return
    * the column reference
    */
  def col[T: SparkType](colName: String)(implicit
      location: Location
  ): F[T] =
    constructSide(SparkType[T].validate(colName), colName)

  /**
    * Retrieves a column with the provided name expecting it to be of string type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the string column reference
    */
  def colString(colName: String)(implicit
      location: Location
  ): F[String] =
    col[String](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of null type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the null column reference
    */
  def colNull(colName: String)(implicit
      location: Location
  ): F[Null] =
    col[Null](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of integer type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the integer column reference
    */
  def colInt(colName: String)(implicit
      location: Location
  ): F[Int] =
    col[Int](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of long type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the long column reference
    */
  def colLong(colName: String)(implicit
      location: Location
  ): F[Long] =
    col[Long](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of double type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the double column reference
    */
  def colDouble(colName: String)(implicit
      location: Location
  ): F[Double] =
    col[Double](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of float type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the float column reference
    */
  def colFloat(colName: String)(implicit
      location: Location
  ): F[Float] =
    col[Float](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of double type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the long column reference
    */
  def colBoolean(colName: String)(implicit
      location: Location
  ): F[Boolean] =
    col[Boolean](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of instant type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the instant column reference
    */
  def colInstant(colName: String)(implicit
      location: Location
  ): F[Instant] =
    col[Instant](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of LocalDate type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the LocalDate column reference
    */
  def colLocalDate(colName: String)(implicit
      location: Location
  ): F[LocalDate] =
    col[LocalDate](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of Timestamp type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the Timestamp column reference
    */
  def colTimestamp(colName: String)(implicit
      location: Location
  ): F[Timestamp] =
    col[Timestamp](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of Date type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the Date column reference
    */
  def colDate(colName: String)(implicit location: Location): F[Date] =
    col[Date](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of array of T type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @tparam T
    * the type of the elements of the array.
    * @return
    * the array of T column reference.
    */
  def colArray[T: ClassTag](colName: String)(implicit
      location: Location,
      st: SparkType[Array[T]]
  ): F[Array[T]] =
    col[Array[T]](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of array of
    * integers type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the array of integers column reference.
    */
  def colArrayInt(colName: String)(implicit
      location: Location
  ): F[Array[Int]] =
    col[Array[Int]](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of array of
    * bytes type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the binary column reference.
    */
  def colBinary(colName: String)(implicit
      location: Location
  ): F[Array[Byte]] =
    col[Array[Byte]](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of array of
    * string type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the array of string column reference.
    */
  def colArrayString(colName: String)(implicit
      location: Location
  ): F[Array[String]] =
    col[Array[String]](colName)

  /**
    * Retrieves a column with the provided name expecting it to be of struct type.
    *
    * @param colName
    * the name of the column to find.
    * @param location
    * error location.
    * @return
    * the struct column reference.
    */
  def colStruct(colName: String)(implicit location: Location): F[Row] =
    col[Row](colName)

  /**
    * Retrieves a column of the provided dataframe. Useful to prevent column
    * ambiguity errors.
    *
    * @param colName
    * the name of the column to find.
    * @param originDF
    * the dataframe to force the column.
    * @param location
    * error location.
    * @tparam T
    * the type of the doric column.
    * @return
    * the column of type T column reference.
    */
  def colFromDF[T: SparkType](colName: String, originDF: Dataset[_])(implicit
      location: Location
  ): F[T] = {
    val doricColumn: Doric[Column] = SparkType[T]
      .validate(colName)
      .run(originDF)
      .fold(DoricColumn[T], DoricColumn[T](_))
      .elem

    constructSide[T](doricColumn, colName)
  }
}
