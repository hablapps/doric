package org.apache.spark.sql

import mrpowers.bebe.Columns._

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedFunction}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, HintInfo, ResolvedHint}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, TimestampFormatter}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.expressions.{Aggregator, SparkUserDefinedFunction, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType.parseTypeWithFallback
import org.apache.spark.util.Utils

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
