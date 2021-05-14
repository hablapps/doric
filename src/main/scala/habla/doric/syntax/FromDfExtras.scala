package habla.doric
package syntax

import cats.data.{Kleisli, Validated}
import cats.implicits.catsSyntaxValidatedIdBinCompat0
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.types.DataType

trait FromDfExtras {

  @inline def dataType[T: FromDf]: DataType = FromDf[T].dataType

  def col[T: FromDf](colName: String)(implicit
      location: Location
  ): DoricColumn[T] =
    FromDf[T].validate(colName)

  def colInt(colName: String)(implicit location: Location): DoricColumn[Int] = {
    FromDf[Int].validate(colName)
  }

  def colLong(
      colName: String
  )(implicit location: Location): DoricColumn[Long] = {
    FromDf[Long].validate(colName)
  }

  def colString(
      colName: String
  )(implicit location: Location): DoricColumn[String] = {
    FromDf[String].validate(colName)
  }

  def colTimestamp(colName: String)(implicit
      location: Location
  ): DoricColumn[Timestamp] =
    FromDf[Timestamp].validate(colName)

  def colDate(colName: String)(implicit location: Location): DoricColumn[Date] =
    FromDf[Date].validate(colName)

  def colArray[T: FromDf](colName: String)(implicit
      location: Location
  ): DoricColumn[Array[T]] =
    FromDf[Array[T]].validate(colName)

  def colArrayInt(colName: String)(implicit
      location: Location
  ): DoricColumn[Array[Int]] =
    FromDf[Array[Int]].validate(colName)

  def colArrayString(colName: String)(implicit
      location: Location
  ): DoricColumn[Array[String]] =
    FromDf[Array[String]].validate(colName)

  def colStruct(colName: String)(implicit location: Location): DStructColumn =
    FromDf[DStruct].validate(colName)

  def colFromDF[T: FromDf](colName: String, originDF: Dataset[_])(implicit
      location: Location
  ): DoricColumn[T] = {
    Kleisli[DoricValidated, Dataset[_], Column](df => {
      val result = FromDf[T].validate(colName).elem.run(originDF)
      if (result.isValid) {
        try {
          val column: Column = result.toEither.right.get
          val head           = df.select(column).schema.head
          if (FromDf[T].isValid(head.dataType))
            Validated.valid(column)
          else
            ColumnTypeError(
              head.name,
              FromDf[T].dataType,
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

object FromDfExtras extends FromDfExtras
