package org.apache.spark.sql

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, HyperLogLogPlusPlus}

object functionsb {

  private def withAggregateFunction(
      func: AggregateFunction,
      isDistinct: Boolean = false): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  def approx_count_distinct(col: Column, rsd: Column = lit(0.05)): Column = withAggregateFunction {
    HyperLogLogPlusPlus(col.expr, rsd.expr.asInstanceOf[Double], 0, 0)
  }

}
