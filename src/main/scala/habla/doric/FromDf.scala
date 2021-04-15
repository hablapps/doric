package habla.doric

import scala.annotation.implicitNotFound

import cats.data.{Kleisli, Validated}
import cats.implicits._

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.DataType

@implicitNotFound(
  "Cant use the type ${T} to generate the typed column. Check your imported FromDf[${T}] instances"
)
trait FromDf[T] {

  def dataType: DataType

  def validate(colName: String): DoricColumn[T] = {
    Kleisli[DoricValidated, DataFrame, Column](df => {
      try {
        val column = df(colName)
        if (column.expr.dataType == dataType)
          Validated.valid(column)
        else
          new Exception(
            s"This column ${column.expr.prettyName} is of type ${column.expr.dataType} and it was expected to be $dataType"
          ).invalidNec
      } catch {
        case e: Throwable => e.invalidNec
      }
    }).toDC
  }

  def isValid(column: Column): Boolean = column.expr.dataType == dataType

}

object FromDf {
  @inline def apply[A: FromDf]: FromDf[A] = implicitly[FromDf[A]]
}
