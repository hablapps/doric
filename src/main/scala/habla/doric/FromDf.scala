package habla.doric

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame}

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Cant use the type ${T} to generate the typed column. Check your imported FromDf[${T}] instances"
)
trait FromDf[T] {

  def dataType: DataType

  def construct: Column => T

  def validate(df: DataFrame, colName: String): T = {
    validate(df(colName))
  }

  def validate(column: Column): T = {
    if (column.expr.dataType == dataType)
      construct(column)
    else
      throw new Exception(
        s"This column ${column.expr.prettyName} is of type ${column.expr.dataType} and it was expected to be $dataType"
      )
  }

  def column: T => Column
}
