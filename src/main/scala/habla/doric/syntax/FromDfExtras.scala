package habla.doric
package syntax

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types.DataType

trait FromDfExtras {

  @inline def dataType[T: FromDf]: DataType = FromDf[T].dataType

  def get[T: FromDf](colName: String)(implicit line: sourcecode.Line, file: sourcecode.FileName): DoricColumn[T] =
    FromDf[T].validate(colName)

  @inline def getInt(colName: String)(implicit line: sourcecode.Line, file: sourcecode.FileName): DoricColumn[Int] =
    get[Int](colName)

  @inline def getString(colName: String)(implicit line: sourcecode.Line, file: sourcecode.FileName): DoricColumn[String] =
    get[String](colName)

  @inline def getTimestamp(colName: String)(implicit line: sourcecode.Line, file: sourcecode.FileName): DoricColumn[Timestamp] =
    get[Timestamp](colName)

  @inline def getDate(colName: String)(implicit line: sourcecode.Line, file: sourcecode.FileName): DoricColumn[Date] =
    get[Date](colName)

  @inline def getArray[T: FromDf](colName: String)(implicit line: sourcecode.Line, file: sourcecode.FileName): DoricColumn[Array[T]] =
    get[Array[T]](colName)

  @inline def getArrayInt(colName: String)(implicit line: sourcecode.Line, file: sourcecode.FileName): DoricColumn[Array[Int]] =
    get[Array[Int]](colName)

  @inline def getArrayString(colName: String)(implicit line: sourcecode.Line, file: sourcecode.FileName): DoricColumn[Array[String]] =
    get[Array[String]](colName)

  @inline def getStruct(colName: String)(implicit line: sourcecode.Line, file: sourcecode.FileName): DStructColumn =
    get[DStruct](colName)

}
