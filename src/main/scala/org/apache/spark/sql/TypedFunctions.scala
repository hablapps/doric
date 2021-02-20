package org.apache.spark.sql

import mrpowers.bebe._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

object TypedFunctions {

  private def withExpr(expr: Expression): Column = Column(expr)
  private def withDateExpr(expr: Expression): DateColumn = DateColumn(Column(expr))
  private def withTimestampExpr(expr: Expression): TimestampColumn = TimestampColumn(Column(expr))

  private def withAggregateFunction(
      func: AggregateFunction,
      isDistinct: Boolean = false): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  def approx_count_distinct(col: Column, rsd: Column = org.apache.spark.sql.functions.lit(0.05)): Column = withAggregateFunction {
    HyperLogLogPlusPlus(col.expr, rsd.expr.asInstanceOf[Double], 0, 0)
  }

  def add_months(startDate: DateColumn, numMonths: IntegerColumn): DateColumn = withDateExpr {
    AddMonths(startDate.col.expr, numMonths.col.expr)
  }

  def add_months(startTime: TimestampColumn, numMonths: IntegerColumn): TimestampColumn = withTimestampExpr {
    AddMonths(startTime.col.expr, numMonths.col.expr)
  }

}
