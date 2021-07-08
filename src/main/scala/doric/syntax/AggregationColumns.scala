package doric
package syntax

import cats.implicits.{catsSyntaxTuple2Semigroupal, toTraverseOps}
import doric.types.{DoubleC, NumericType}

import org.apache.spark.sql.{functions => f}

trait AggregationColumns {

  def sum[T: NumericType](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.sum).toDC

  def count(col: DoricColumn[_]): LongColumn =
    col.elem.map(f.count).toDC

  def first[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.first).toDC

  def first[T](col: DoricColumn[T], ignoreNulls: Boolean): DoricColumn[T] =
    col.elem.map(f.first(_, ignoreNulls)).toDC

  def last[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.last).toDC

  def last[T](col: DoricColumn[T], ignoreNulls: Boolean): DoricColumn[T] =
    col.elem.map(f.last(_, ignoreNulls)).toDC

  def aproxCountDistinct(col: DoricColumn[_], rsd: Double): LongColumn =
    col.elem.map(f.approx_count_distinct(_, rsd)).toDC

  def aproxCountDistinct(col: DoricColumn[_]): LongColumn =
    col.elem.map(f.approx_count_distinct).toDC

  def aproxCountDistinct(colName: String, rsd: Double): LongColumn =
    aproxCountDistinct(DoricColumn.uncheckedType(f.col(colName)), rsd)

  def aproxCountDistinct(colName: String): LongColumn =
    aproxCountDistinct(DoricColumn.uncheckedType(f.col(colName)))

  def avg[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.avg).toDC

  def collect_list[T](col: DoricColumn[T]): DoricColumn[Array[T]] =
    col.elem.map(f.collect_list).toDC

  def collect_set[T](col: DoricColumn[T]): DoricColumn[Array[T]] =
    col.elem.map(f.collect_set).toDC

  def corr(col1: DoubleColumn, col2: DoubleColumn): DoubleColumn =
    (col1.elem, col2.elem).mapN(f.corr).toDC

  def countDistinct(expr: DoricColumn[_], exprs: DoricColumn[_]*): LongColumn =
    (expr +: exprs).toList
      .traverse(_.elem)
      .map(x => f.countDistinct(x.head, x.tail: _*))
      .toDC

  def covar_pop(col1: DoubleColumn, col2: DoubleColumn): DoubleColumn =
    (col1.elem, col2.elem).mapN(f.covar_pop).toDC

  def covar_samp(col1: DoubleColumn, col2: DoubleColumn): DoubleColumn =
    (col1.elem, col2.elem).mapN(f.covar_samp).toDC

  def kurtosis(col: DoubleColumn): DoubleColumn =
    col.elem.map(f.kurtosis).toDC

  def max[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.max).toDC

  def min[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.min).toDC

  def mean[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.mean).toDC

  def percentile_approx[T: DoubleC](
      col: DoricColumn[T],
      percentage: Array[Double],
      accuracy: Int
  ): DoricColumn[Array[T]] =
    col.elem
      .map(f.percentile_approx(_, f.lit(percentage), f.lit(accuracy)))
      .toDC

  def percentile_approx[T: DoubleC](
      col: DoubleColumn,
      percentage: Double,
      accuracy: Int
  ): DoricColumn[T] =
    col.elem
      .map(f.percentile_approx(_, f.lit(percentage), f.lit(accuracy)))
      .toDC
}
