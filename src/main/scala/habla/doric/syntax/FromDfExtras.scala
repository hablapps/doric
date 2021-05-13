package habla.doric
package syntax

import java.sql.{Date, Timestamp}

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

}

object FromDfExtras extends FromDfExtras
