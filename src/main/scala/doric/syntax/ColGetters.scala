package doric.syntax

import doric.{Doric, DoricColumn, DStruct}
import doric.sem.Location
import doric.types.SparkType
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.{Column, Dataset}

private[doric] trait ColGetters[F[_]] {
  @inline protected def constructSide[T](column: Doric[Column]): F[T]

  def col[T: SparkType](colName: String)(implicit
      location: Location
  ): F[T] =
    constructSide(SparkType[T].validate(colName))

  def colString(colName: String)(implicit
      location: Location
  ): F[String] =
    col[String](colName)

  def colInt(colName: String)(implicit
      location: Location
  ): F[Int] =
    col[Int](colName)

  def colLong(colName: String)(implicit
      location: Location
  ): F[Long] =
    col[Long](colName)

  def colInstant(colName: String)(implicit
      location: Location
  ): F[Instant] =
    col[Instant](colName)

  def colLocalDate(colName: String)(implicit
      location: Location
  ): F[LocalDate] =
    col[LocalDate](colName)

  def colTimestamp(colName: String)(implicit
      location: Location
  ): F[Timestamp] =
    col[Timestamp](colName)

  def colDate(colName: String)(implicit location: Location): F[Date] =
    col[Date](colName)

  def colArray[T: SparkType](colName: String)(implicit
      location: Location
  ): F[Array[T]] =
    col[Array[T]](colName)

  def colArrayInt(colName: String)(implicit
      location: Location
  ): F[Array[Int]] =
    col[Array[Int]](colName)

  def colArrayString(colName: String)(implicit
      location: Location
  ): F[Array[String]] =
    col[Array[String]](colName)

  def colStruct(colName: String)(implicit location: Location): F[DStruct] =
    col[DStruct](colName)

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
