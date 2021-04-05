package habla.doric
package functions

import cats.implicits._

import org.apache.spark.sql.functions.{lit, when}
final case class WhenBuilder[T](
    private val cases: Vector[(BooleanColumn, DoricColumn[T])] = Vector.empty
) {

  type Lit[ST] = Literal[T, ST]

  private def casesToWhenColumn: DoricColumn[T] = {
    val first = cases.head
    cases.tail.foldLeft(first._1.mapN[T, T](first._2)(when))((acc, c) =>
      DoricColumn((acc.toKleisli, c._1.toKleisli, c._2.toKleisli).mapN(_.when(_, _)).run)
    )
  }
  def caseW(cond: BooleanColumn, elem: DoricColumn[T]): WhenBuilder[T] =
    WhenBuilder(cases.:+((cond, elem)))
  def caseW[LT: Lit](cond: BooleanColumn, elem: LT): WhenBuilder[T] =
    WhenBuilder(cases.:+((cond, elem.lit)))
  def otherwiseNull(implicit dt: FromDf[T]): DoricColumn[T] =
    if (cases.isEmpty)
      DoricColumn(_ => lit(null).cast(dataType[T]).valid)
    else
      casesToWhenColumn

  def otherwise(other: DoricColumn[T]): DoricColumn[T] =
    if (cases.isEmpty) other
    else casesToWhenColumn.mapN(other)(_.otherwise(_))

  def otherwise[LT: Lit](other: LT): DoricColumn[T] =
    if (cases.isEmpty)
      other.lit
    else
      casesToWhenColumn.mapN(other.lit)(_.otherwise(_))
}
