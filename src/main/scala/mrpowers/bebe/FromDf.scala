package mrpowers.bebe

import scala.annotation.implicitNotFound

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame}

@implicitNotFound(
  "Cant use the type ${T} to generate the typed column. Check your imported FromDf[${T}] instances"
)
trait FromDf[T] {

  def dataType: DataType

  def construct(column: Column): T

  def validate(df: DataFrame, colName: String): T = {
    val column = df(colName)
    if (column.expr.dataType == dataType)
      construct(column)
    else
      throw new Exception(
        s"The column $colName in the dataframe is of type ${column.expr.dataType} and it was expected to be $dataType"
      )
  }

  def validate(column: Column): T = {
    if (column.expr.dataType == dataType)
      construct(column)
    else
      throw new Exception(
        s"This column ${column.expr.prettyName} is of type ${column.expr.dataType} and it was expected to be $dataType"
      )
  }
}
