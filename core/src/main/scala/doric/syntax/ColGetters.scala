package doric
package syntax

import doric.sem.Location
import doric.types.SparkType
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.{Column, Dataset}

private[doric] trait ColGetters[F[_]] {
  @inline protected def constructSide[T](column: Doric[Column]): F[T]

  /**
    * Retreaves a column with the provided name and the provided type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @tparam T the expected type of the column
    * @return the column reference
    */
  def col[T: SparkType](colName: String)(implicit
      location: Location
  ): F[T] =
    constructSide(SparkType[T].validate(colName))

  /**
    * Retreaves a column with the provided name expecting it to be of string type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @return the string column reference
    */
  def colString(colName: String)(implicit
      location: Location
  ): F[String] =
    col[String](colName)

  /**
    * Retreaves a column with the provided name expecting it to be of integer type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @return the integer column reference
    */
  def colInt(colName: String)(implicit
      location: Location
  ): F[Int] =
    col[Int](colName)

  /**
    * Retreaves a column with the provided name expecting it to be of long type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @return the long column reference
    */
  def colLong(colName: String)(implicit
      location: Location
  ): F[Long] =
    col[Long](colName)

  /**
    * Retreaves a column with the provided name expecting it to be of instant type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @return the instant column reference
    */
  def colInstant(colName: String)(implicit
      location: Location
  ): F[Instant] =
    col[Instant](colName)

  /**
    * Retreaves a column with the provided name expecting it to be of LocalDate type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @return the LocalDate column reference
    */
  def colLocalDate(colName: String)(implicit
      location: Location
  ): F[LocalDate] =
    col[LocalDate](colName)

  /**
    * Retreaves a column with the provided name expecting it to be of Timestamp type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @return the Timestamp column reference
    */
  def colTimestamp(colName: String)(implicit
      location: Location
  ): F[Timestamp] =
    col[Timestamp](colName)

  /**
    * Retreaves a column with the provided name expecting it to be of Date type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @return the Date column reference
    */
  def colDate(colName: String)(implicit location: Location): F[Date] =
    col[Date](colName)

  /**
    * Retreaves a column with the provided name expecting it to be of array of T type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @tparam T the type of the elements of the array.
    * @return the array of T column reference.
    */
  def colArray[T: SparkType](colName: String)(implicit
      location: Location
  ): F[Array[T]] =
    col[Array[T]](colName)

  /**
    * Retreaves a column with the provided name expecting it to be of array of integers type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @return the array of integers column reference.
    */
  def colArrayInt(colName: String)(implicit
      location: Location
  ): F[Array[Int]] =
    col[Array[Int]](colName)

  /**
    * Retreaves a column with the provided name expecting it to be of array of string type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @return the array of string column reference.
    */
  def colArrayString(colName: String)(implicit
      location: Location
  ): F[Array[String]] =
    col[Array[String]](colName)

  /**
    * Retreaves a column with the provided name expecting it to be of struct type.
    * @param colName the name of the column to find.
    * @param location error location.
    * @return the struct column reference.
    */
  def colStruct(colName: String)(implicit location: Location): F[DStruct] =
    col[DStruct](colName)

  /**
    * Retreaves a column of the provided dataframe. Useful to prevent column ambiguity errors.
    * @param colName the name of the column to find.
    * @param originDF the dataframe to force the column.
    * @param location error location.
    * @tparam T the type of the doric column.
    * @return the column of type T column reference.
    */
  def colFromDF[T: SparkType](colName: String, originDF: Dataset[_])(implicit
      location: Location
  ): F[T] = {
    val doricColumn: Doric[Column] = SparkType[T]
      .validate(colName)
      .run(originDF)
      .fold(DoricColumn[T], DoricColumn[T](_))
      .elem

    constructSide[T](doricColumn)
  }
}
