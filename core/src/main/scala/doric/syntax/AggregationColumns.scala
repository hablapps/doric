package doric
package syntax

import cats.implicits.{catsSyntaxTuple2Semigroupal, toTraverseOps}
import doric.types.{DoubleC, NumericType}

import org.apache.spark.sql.{functions => f}

private[syntax] trait AggregationColumns {

  /**
    * @group Aggregation Numeric Type
    */
  def sum[T: NumericType](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.sum).toDC

  /**
    * @group Aggregation Any Type
    */
  def count(col: DoricColumn[_]): LongColumn =
    col.elem.map(f.count).toDC

  def count(colName: CName): LongColumn =
    Doric.unchecked(colName).map(f.count).toDC

  /**
    * @group Aggregation Any Type
    */
  def first[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.first).toDC

  /**
    * @group Aggregation Any Type
    */
  def first[T](col: DoricColumn[T], ignoreNulls: Boolean): DoricColumn[T] =
    col.elem.map(f.first(_, ignoreNulls)).toDC

  /**
    * @group Aggregation Any Type
    */
  def last[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.last).toDC

  /**
    * @group Aggregation Any Type
    */
  def last[T](col: DoricColumn[T], ignoreNulls: Boolean): DoricColumn[T] =
    col.elem.map(f.last(_, ignoreNulls)).toDC

  /**
    * @group Aggregation Any Type
    */
  def aproxCountDistinct(col: DoricColumn[_], rsd: Double): LongColumn =
    col.elem.map(f.approx_count_distinct(_, rsd)).toDC

  /**
    * @group Aggregation Any Type
    */
  def aproxCountDistinct(col: DoricColumn[_]): LongColumn =
    col.elem.map(f.approx_count_distinct).toDC

  /**
    * @group Aggregation Any Type
    */
  def aproxCountDistinct(colName: String, rsd: Double): LongColumn =
    aproxCountDistinct(DoricColumn.uncheckedType(f.col(colName)), rsd)

  /**
    * @group Aggregation Any Type
    */
  def aproxCountDistinct(colName: String): LongColumn =
    aproxCountDistinct(DoricColumn.uncheckedType(f.col(colName)))

  /**
    * @group Aggregation Numeric Type
    */
  def avg[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.avg).toDC

  /**
    * @group Aggregation Any Type
    */
  def collect_list[T](col: DoricColumn[T]): DoricColumn[Array[T]] =
    col.elem.map(f.collect_list).toDC

  /**
    * @group Aggregation Any Type
    */
  def collect_set[T](col: DoricColumn[T]): DoricColumn[Array[T]] =
    col.elem.map(f.collect_set).toDC

  /**
    * @group Aggregation Any Type
    */
  def corr(col1: DoubleColumn, col2: DoubleColumn): DoubleColumn =
    (col1.elem, col2.elem).mapN(f.corr).toDC

  /**
    * @group Aggregation Any Type
    */
  def countDistinct(expr: DoricColumn[_], exprs: DoricColumn[_]*): LongColumn =
    (expr +: exprs).toList
      .traverse(_.elem)
      .map(x => f.countDistinct(x.head, x.tail: _*))
      .toDC

  /**
    * @group Aggregation Double Type
    */
  def covar_pop(col1: DoubleColumn, col2: DoubleColumn): DoubleColumn =
    (col1.elem, col2.elem).mapN(f.covar_pop).toDC

  /**
    * @group Aggregation Double Type
    */
  def covar_samp(col1: DoubleColumn, col2: DoubleColumn): DoubleColumn =
    (col1.elem, col2.elem).mapN(f.covar_samp).toDC

  /**
    * @group Aggregation Double Type
    */
  def kurtosis(col: DoubleColumn): DoubleColumn =
    col.elem.map(f.kurtosis).toDC

  def max[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.max).toDC

  def min[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.min).toDC

  def mean[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.mean).toDC

  /**
    * @group Aggregation DoubleC Type
    */
  def percentile_approx[T: DoubleC](
      col: DoricColumn[T],
      percentage: Array[Double],
      accuracy: Int
  ): DoricColumn[Array[T]] =
    col.elem
      .map(f.percentile_approx(_, f.lit(percentage), f.lit(accuracy)))
      .toDC

  /**
    * @group Aggregation DoubleC Type
    */
  def percentile_approx[T: DoubleC](
      col: DoubleColumn,
      percentage: Double,
      accuracy: Int
  ): DoricColumn[T] =
    col.elem
      .map(f.percentile_approx(_, f.lit(percentage), f.lit(accuracy)))
      .toDC
}
