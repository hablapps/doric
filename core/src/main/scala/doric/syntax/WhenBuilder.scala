package doric
package syntax

import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxTuple2Semigroupal, catsSyntaxTuple3Semigroupal}
import doric.types.SparkType

import org.apache.spark.sql.functions.{lit => sparkLit, when => sparkWhen}
import org.apache.spark.sql.Column

final private[doric] case class WhenBuilder[T](
    private[doric] val cases: Vector[(BooleanColumn, DoricColumn[T])] =
      Vector.empty
) {

  /**
    * Marks the rest of cases as null values of the provided type
    * @param dt Type class for spark data type
    * @return The doric column with the provided logic in the builder
    */
  def otherwiseNull(implicit dt: SparkType[T]): DoricColumn[T] =
    if (cases.isEmpty)
      sparkLit(null).cast(dataType[T]).pure[Doric].toDC
    else
      casesToWhenColumn.toDC

  /**
    * ads a case that if the condition is matched, the value is returned
    * @param cond BooleanColumn with the condition to satify
    * @param elem the returned element if the condition is true
    * @return new instance of the builder with the previous cases added
    */
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

  /**
    * For the rest of cases adds a default value
    * @param other the default value to return
    * @return The doric column with the provided logic in the builder
    */
  def otherwise(other: DoricColumn[T]): DoricColumn[T] =
    if (cases.isEmpty) other
    else (casesToWhenColumn, other.elem).mapN(_.otherwise(_)).toDC

}
