package habla.doric

import org.apache.spark.sql.Column

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Cant use the type ${T} to extract a spark column from the typed column. Check your imported ToColumn[${T}] instances"
)
trait ToColumn[T] {
  def column(typedCol: T): Column
}
