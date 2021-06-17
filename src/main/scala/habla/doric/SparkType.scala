package habla.doric

import scala.annotation.implicitNotFound

import cats.data.{Kleisli, Validated}
import cats.implicits._

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.types.DataType

/**
  * Typeclass to relate a type T with it's spark DataType
  * @tparam T the scala type of the instance
  */
@implicitNotFound(
  "Cant use the type ${T} to generate the typed column. Check your imported SparkType[${T}] instances"
)
trait SparkType[T] {

  /**
    * The spark DataType
    * @return the spark DataType
    */
  def dataType: DataType

  /**
    * Validates if a column of the in put dataframe exist and is of the spark DataType provided
    * @param colName name of the column to extract
    * @param location object that links to the position if an error is created
    * @return A Doric column that validates al the logic
    */
  def validate(colName: String)(implicit location: Location): Doric[Column] = {
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
    })
  }

  /**
    * Checks if the datatype corresponds to the provided datatype, but skipping if can be null
    * @param column the datatype to check
    * @return true if the datatype is equal to the one of the typeclass
    */
  def isValid(column: DataType): Boolean = column == dataType

}

object SparkType {
  @inline def apply[A: SparkType]: SparkType[A] = implicitly[SparkType[A]]
}
