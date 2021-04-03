package habla.doric

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame}

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Cant use the type ${T} to generate the typed column. Check your imported FromDf[${T}] instances"
)
trait FromDf[T] {

  def dataType: DataType

  def validate(df: DataFrame, colName: String): DoricColumn[T] = {
    validate(df(colName))
  }

  def validate(column: Column): DoricColumn[T] = {
    if (isValid(column))
      DoricColumn(column)
    else
      throw new Exception(
        s"This column ${column.expr.prettyName} is of type ${column.expr.dataType} and it was expected to be $dataType"
      )
  }

  def isValid(column: Column): Boolean = column.expr.dataType == dataType

}

object FromDf {
  @inline def apply[A: FromDf] = implicitly[FromDf[A]]
}
