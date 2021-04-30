package habla.doric
package syntax

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types.DataType

trait FromDfExtras {

  @inline def dataType[T: FromDf]: DataType = FromDf[T].dataType

  def get[T: FromDf](colName: String)(implicit location: Location): DoricColumn[T] =
    FromDf[T].validate(colName)

  def getInt(colName: String)(implicit location: Location): DoricColumn[Int] = {
    FromDf[Int].validate(colName)
  }

  def getString(colName: String)(implicit location: Location): DoricColumn[String] = {
    FromDf[String].validate(colName)
  }

  def getTimestamp(colName: String)(implicit location: Location): DoricColumn[Timestamp] =
    FromDf[Timestamp].validate(colName)

  def getDate(colName: String)(implicit location: Location): DoricColumn[Date] =
    FromDf[Date].validate(colName)

  def getArray[T: FromDf](colName: String)(implicit location: Location): DoricColumn[Array[T]] =
    FromDf[Array[T]].validate(colName)

  def getArrayInt(colName: String)(implicit location: Location): DoricColumn[Array[Int]] =
    FromDf[Array[Int]].validate(colName)

  def getArrayString(colName: String)(implicit location: Location): DoricColumn[Array[String]] =
    FromDf[Array[String]].validate(colName)

  def getStruct(colName: String)(implicit location: Location): DStructColumn =
    FromDf[DStruct].validate(colName)

}
