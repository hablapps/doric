package habla.doric

import scala.annotation.implicitNotFound

import cats.data.{Kleisli, Validated}
import cats.implicits._

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.types.DataType

@implicitNotFound(
  "Cant use the type ${T} to generate the typed column. Check your imported FromDf[${T}] instances"
)
trait FromDf[T] {

  def dataType: DataType

  def validate(colName: String)(implicit location: Location): DoricColumn[T] = {
    Kleisli[DoricValidated, Dataset[_], Column](df => {
      try {
        val column = df(colName)
        if (isValid(column.expr.dataType))
          Validated.valid(column)
        else
          ColumnTypeError(colName, dataType, column.expr.dataType).invalidNec
      } catch {
        case e: Throwable => SparkErrorWrapper(e).invalidNec
      }
    }).toDC
  }

  /**
    * Checks if the datatype corresponds to the provided datatype, but skipping if can be null
    * @param column the datatype to check
    * @return true if the datatype is equal to the one of the typeclass
    */
  def isValid(column: DataType): Boolean = column == dataType

}

object FromDf {
  @inline def apply[A: FromDf]: FromDf[A] = implicitly[FromDf[A]]
}
