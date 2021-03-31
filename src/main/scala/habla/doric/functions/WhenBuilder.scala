package habla.doric
package functions

import scala.collection.immutable.Queue
import habla.doric.BooleanColumn
import habla.doric.FromDf

import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.Column

final case class WhenBuilder[T](
    private val cases: Vector[(BooleanColumn, DoricColumn[T])] = Vector.empty
) {

  type Lit[ST] = Literal[T, ST]

  private def casesToWhenColumn: Column = {
    val first = cases.head
    cases.tail.foldLeft(when(first._1.col, first._2.col))(
      (acc, c) => acc.when(c._1.col, c._2.col)
    )
  }
  def caseW(cond: BooleanColumn, elem: DoricColumn[T]): WhenBuilder[T] =
    WhenBuilder(cases.:+((cond, elem)))
  def caseW[LT: Lit](cond: BooleanColumn, elem: LT): WhenBuilder[T] =
    WhenBuilder(cases.:+((cond, elem.lit)))
  def otherwiseNull(implicit dt: FromDf[T]): DoricColumn[T] =
    if (cases.isEmpty)
      DoricColumn(lit(null).cast(dataType[T]))
    else
      DoricColumn(casesToWhenColumn)

  def otherwise(other: DoricColumn[T]): DoricColumn[T] = if (cases.isEmpty) other
  else DoricColumn(casesToWhenColumn.otherwise(other.col))
  def otherwise[LT: Lit](other: LT): DoricColumn[T] =
    if (cases.isEmpty)
      other.lit
    else
      DoricColumn(casesToWhenColumn.otherwise(other.lit.col))
}
