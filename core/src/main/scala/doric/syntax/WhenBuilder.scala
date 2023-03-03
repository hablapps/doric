package doric
package syntax

import cats.implicits._
import doric.types.{Casting, SparkType}

import org.apache.spark.sql.functions.{when => sparkWhen}
import org.apache.spark.sql.Column

final private[doric] case class WhenBuilder[T](
    private[doric] val cases: Vector[(BooleanColumn, DoricColumn[T])] =
      Vector.empty
) {

  /**
    * Marks the rest of cases as null values of the provided type
    *
    * @param dt
    *   Type class for spark data type
    * @return
    *   The doric column with the provided logic in the builder
    */
  def otherwiseNull(implicit
      dt: SparkType[T],
      C: Casting[Null, T]
  ): DoricColumn[T] =
    if (cases.isEmpty) {
      lit(null).cast[T]
    } else
      casesToWhenColumn.toDC

  /**
    * ads a case that if the condition is matched, the value is returned
    * @param cond
    *   BooleanColumn with the condition to satisfy
    * @param elem
    *   the returned element if the condition is true
    * @return
    *   new instance of the builder with the previous cases added
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
    * @param other
    *   the default value to return
    * @return
    *   The doric column with the provided logic in the builder
    */
  def otherwise(other: DoricColumn[T]): DoricColumn[T] =
    if (cases.isEmpty) other
    else (casesToWhenColumn, other.elem).mapN(_.otherwise(_)).toDC

}

final protected case class MatchBuilderInit[O: SparkType, T](
    private val dCol: DoricColumn[O]
) {

  /**
    * ads the first case comparing the given column with the affected column
    *
    * @param equality
    * the given column to compare
    * @param elem
    * the returned element if the condition is true
    * @return
    * new instance of the builder with the previous cases added
    */
  def caseW(
      equality: DoricColumn[O],
      elem: DoricColumn[T]
  ): MatchBuilder[O, T] =
    MatchBuilder(dCol, when[T].caseW(dCol === equality, elem))

  /**
    * ads the first case using a function to compare the given column and the affected column
    *
    * @param function
    * BooleanColumn with the condition to satisfy
    * @param elem
    * the returned element if the condition is true
    * @return
    * new instance of the builder with the previous cases added
    */
  def caseW(
      function: DoricColumn[O] => BooleanColumn,
      elem: DoricColumn[T]
  ): MatchBuilder[O, T] =
    MatchBuilder(dCol, when[T].caseW(function(dCol), elem))

}

final protected case class MatchBuilder[O: SparkType, T](
    private val dCol: DoricColumn[O],
    private val cases: WhenBuilder[T]
) {

  /**
    * ads a case comparing the given column with the affected column
    *
    * @param equality
    * the given column to compare
    * @param elem
    * the returned element if the condition is true
    * @return
    * new instance of the builder with the previous cases added
    */
  def caseW(
      equality: DoricColumn[O],
      elem: DoricColumn[T]
  ): MatchBuilder[O, T] =
    MatchBuilder(dCol, cases.caseW(dCol === equality, elem))

  /**
    * ads a case using a function to compare the given column and the affected column
    *
    * @param function
    * BooleanColumn with the condition to satisfy
    * @param elem
    * the returned element if the condition is true
    * @return
    * new instance of the builder with the previous cases added
    */
  def caseW(
      function: DoricColumn[O] => BooleanColumn,
      elem: DoricColumn[T]
  ): MatchBuilder[O, T] =
    MatchBuilder(dCol, cases.caseW(function(dCol), elem))

  /**
    * Marks the rest of cases as null values of the provided type
    *
    * @param dt
    *   Type class for spark data type
    * @return
    *   The doric column with the provided logic in the builder
    */
  def otherwiseNull(implicit dt: SparkType[T]): DoricColumn[T] =
    cases.otherwiseNull

  /**
    * For the rest of cases adds a default value
    * @param other
    *   the default value to return
    * @return
    *   The doric column with the provided logic in the builder
    */
  def otherwise(other: DoricColumn[T]): DoricColumn[T] =
    cases.otherwise(other)

}
