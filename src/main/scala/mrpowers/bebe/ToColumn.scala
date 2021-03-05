package mrpowers.bebe

import scala.annotation.implicitNotFound

import org.apache.spark.sql.Column

@implicitNotFound(
  "Cant use the type ${T} to extract a spark column from the typed column. Check your imported ToColumn[${T}] instances"
)
trait ToColumn[T] {
  def column(typedCol: T): Column
}
