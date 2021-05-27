package habla.doric
package syntax

import cats.data.{Kleisli, Validated}
import cats.implicits.catsSyntaxValidatedIdBinCompat0
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.types.DataType

trait ColumnExtractors {

  @inline def dataType[T: SparkType]: DataType = SparkType[T].dataType

  def col[T: SparkType](colName: String)(implicit
                                         location: Location
  ): DoricColumn[T] =
    SparkType[T].validate(colName)

  def colInt(colName: String)(implicit location: Location): DoricColumn[Int] = {
    SparkType[Int].validate(colName)
  }

  def colLong(
      colName: String
  )(implicit location: Location): DoricColumn[Long] = {
    SparkType[Long].validate(colName)
  }

  def colString(
      colName: String
  )(implicit location: Location): DoricColumn[String] = {
    SparkType[String].validate(colName)
  }

  def colTimestamp(colName: String)(implicit
      location: Location
  ): DoricColumn[Timestamp] =
    SparkType[Timestamp].validate(colName)

  def colDate(colName: String)(implicit location: Location): DoricColumn[Date] =
    SparkType[Date].validate(colName)

  def colArray[T: SparkType](colName: String)(implicit
                                              location: Location
  ): DoricColumn[Array[T]] =
    SparkType[Array[T]].validate(colName)

  def colArrayInt(colName: String)(implicit
      location: Location
  ): DoricColumn[Array[Int]] =
    SparkType[Array[Int]].validate(colName)

  def colArrayString(colName: String)(implicit
      location: Location
  ): DoricColumn[Array[String]] =
    SparkType[Array[String]].validate(colName)

  def colStruct(colName: String)(implicit location: Location): DStructColumn =
    SparkType[DStruct].validate(colName)

  def colFromDF[T: SparkType](colName: String, originDF: Dataset[_])(implicit
                                                                     location: Location
  ): DoricColumn[T] = {
    Kleisli[DoricValidated, Dataset[_], Column](df => {
      val result = SparkType[T].validate(colName).elem.run(originDF)
      if (result.isValid) {
        try {
          val column: Column = result.toEither.right.get
          val head           = df.select(column).schema.head
          if (SparkType[T].isValid(head.dataType))
            Validated.valid(column)
          else
            ColumnTypeError(
              head.name,
              SparkType[T].dataType,
              head.dataType
            ).invalidNec
        } catch {
          case e: Throwable => SparkErrorWrapper(e).invalidNec
        }
      } else {
        result
      }
    }).toDC
  }

}

object ColumnExtractors extends ColumnExtractors
