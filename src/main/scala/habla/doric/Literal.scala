package habla.doric

import scala.annotation.implicitNotFound

import cats.implicits._

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

@implicitNotFound("The type $ScalaType cant be casted as a literal for $T.")
trait Literal[T, ScalaType] {
  def createLiteral(st: ScalaType): Column = lit(st)

  def createTLiteral(st: ScalaType): DoricColumn[T] =
    createLiteral(st).pure[Doric].toDC

}

object Literal {
  @inline def apply[T, ScalaType](implicit imp: Literal[T, ScalaType]): Literal[T, ScalaType] = {
    imp
  }
}
