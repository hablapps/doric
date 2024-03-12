package doric
package syntax

import cats.implicits._
import doric.sqlExpressions.CustomAgg
import doric.types.SparkType

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.catalyst.expressions.Expression

trait AggregationColumns32 {
  def customAgg[T, A: SparkType, E](
      column: DoricColumn[T],
      initial: DoricColumn[A],
      update: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A],
      merge: (DoricColumn[A], DoricColumn[A]) => DoricColumn[A],
      evaluate: DoricColumn[A] => DoricColumn[E]
  ): DoricColumn[E] = {
    val updateExpr: Doric[(Expression, Expression) => Expression] =
      Doric((df: Dataset[_]) =>
        (e1: Expression, e2: Expression) =>
          update(Doric(new Column(e1)).toDC, Doric(new Column(e2)).toDC).elem
            .run(df)
            .map(_.expr)
            .getOrElse(null)
      )
    val mergeExpr = Doric((df: Dataset[_]) =>
      (e1: Expression, e2: Expression) =>
        merge(Doric(new Column(e1)).toDC, Doric(new Column(e2)).toDC).elem
          .run(df.sparkSession.emptyDataFrame)
          .map(_.expr)
          .getOrElse(null)
    )
    val evaluateExpr = Doric((df: Dataset[_]) =>
      (e1: Expression) =>
        evaluate(Doric(new Column(e1)).toDC).elem
          .run(df.sparkSession.emptyDataFrame)
          .map(_.expr)
          .getOrElse(null)
    )
    (
      column.elem,
      initial.elem,
      updateExpr,
      mergeExpr,
      evaluateExpr,
      update(initial, column).elem,
      merge(initial, initial).elem.lmap[Dataset[_]](df =>
        df.sparkSession.emptyDataFrame
      ),
      evaluate(initial).elem.lmap[Dataset[_]](df =>
        df.sparkSession.emptyDataFrame
      )
    ).mapN { (c, i, u, m, e, _, _, _) =>
      new Column(
        CustomAgg(
          c.expr,
          i.expr,
          u,
          m,
          e
        ).toAggregateExpression(isDistinct = false)
      )
    }.toDC
  }
}
