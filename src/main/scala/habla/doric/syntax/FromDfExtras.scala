package habla.doric
package syntax

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types.DataType

trait FromDfExtras {

  @inline def dataType[T: FromDf]: DataType = FromDf[T].dataType

  def get[T: FromDf](colName: String): DoricColumn[T] =
    FromDf[T].validate(colName)

  @inline def getInt(colName: String): DoricColumn[Int] =
    get[Int](colName)

  @inline def getString(colName: String): DoricColumn[String] =
    get[String](colName)

  @inline def getTimestamp(colName: String): DoricColumn[Timestamp] =
    get[Timestamp](colName)

  @inline def getDate(colName: String): DoricColumn[Date] =
    get[Date](colName)

  @inline def getArray[T: FromDf](colName: String): DoricColumn[Array[T]] =
    get[Array[T]](colName)

  @inline def getArrayInt(colName: String): DoricColumn[Array[Int]] =
    get[Array[Int]](colName)

  @inline def getArrayString(colName: String): DoricColumn[Array[String]] =
    get[Array[String]](colName)

  @inline def getStruct(colName: String): DStructColumn =
    get[DStruct](colName)

}
