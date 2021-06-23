package doric
package control

import cats.implicits._
import doric.types.SparkType

import org.apache.spark.sql.functions.{lit => sparkLit, when => sparkWhen}
import org.apache.spark.sql.Column

final private[doric] case class WhenBuilder[T](
    private[doric] val cases: Vector[(BooleanColumn, DoricColumn[T])] =
      Vector.empty
) {

  def otherwiseNull(implicit dt: SparkType[T]): DoricColumn[T] =
    if (cases.isEmpty)
      sparkLit(null).cast(dataType[T]).pure[Doric].toDC
    else
      casesToWhenColumn.toDC

  def caseW(cond: BooleanColumn, elem: DoricColumn[T]): WhenBuilder[T] =
    WhenBuilder(cases.:+((cond, elem)))

  private def casesToWhenColumn: Doric[Column] = {
    val first = cases.head
    cases.tail.foldLeft(
      (first._1.elem, first._2.elem).mapN((c, a) => sparkWhen(c, a))
    )((acc, c) =>
      (acc, c._1.elem, c._2.elem).mapN((a, cond, algo) => a.when(cond, algo))
    )
  }

  def otherwise(other: DoricColumn[T]): DoricColumn[T] =
    if (cases.isEmpty) other
    else (casesToWhenColumn, other.elem).mapN(_.otherwise(_)).toDC

}
