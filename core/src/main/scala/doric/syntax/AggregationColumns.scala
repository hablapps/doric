package doric
package syntax

import cats.implicits._
import doric.types.NumericType
import doric.Doric

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum

private[syntax] trait AggregationColumns {

  /**
    * Aggregate function: returns the sum of all values in the expression.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.sum(e:* org.apache.spark.sql.functions.sum]]
    */
  def sum[T](col: DoricColumn[T])(implicit
      nt: NumericType[T]
  ): DoricColumn[nt.Sum] =
    col.elem.map(f.sum).toDC

  /**
    * Aggregate function: returns the number of items in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.count(e:* org.apache.spark.sql.functions.count]]
    */
  def count(col: DoricColumn[_]): LongColumn =
    col.elem.map(f.count).toDC

  /**
    * Aggregate function: returns the number of items in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.count(columnName:* org.apache.spark.sql.functions.count]]
    */
  def count(colName: CName): LongColumn =
    DoricColumn.uncheckedType(colName.value).elem.map(f.count).toDC

  /**
    * Aggregate function: returns the first value in a group.
    *
    * The function by default returns the first values it sees. It will return the first non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @note The function is non-deterministic because its results depends on the order of the rows
    * which may be non-deterministic after a shuffle.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.first(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.first]]
    */
  def first[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.first).toDC

  /**
    * Aggregate function: returns the first value in a group.
    *
    * The function by default returns the first values it sees. It will return the first non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @note The function is non-deterministic because its results depends on the order of the rows
    * which may be non-deterministic after a shuffle.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.first(e:org\.apache\.spark\.sql\.Column,ignoreNulls:* org.apache.spark.sql.functions.first]]
    */
  def first[T](col: DoricColumn[T], ignoreNulls: Boolean): DoricColumn[T] =
    col.elem.map(f.first(_, ignoreNulls)).toDC

  /**
    * Aggregate function: returns the last value in a group.
    *
    * The function by default returns the last values it sees. It will return the last non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @note The function is non-deterministic because its results depends on the order of the rows
    * which may be non-deterministic after a shuffle.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.last(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.last]]
    */
  def last[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.last).toDC

  /**
    * Aggregate function: returns the last value in a group.
    *
    * The function by default returns the last values it sees. It will return the last non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @note The function is non-deterministic because its results depends on the order of the rows
    * which may be non-deterministic after a shuffle.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.last(e:org\.apache\.spark\.sql\.Column,ignoreNulls:* org.apache.spark.sql.functions.last]]
    */
  def last[T](col: DoricColumn[T], ignoreNulls: Boolean): DoricColumn[T] =
    col.elem.map(f.last(_, ignoreNulls)).toDC

  /**
    * Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.approx_count_distinct(e:org\.apache\.spark\.sql\.Column,rsd:* org.apache.spark.sql.functions.approx_count_distinct]]
    */
  def aproxCountDistinct(col: DoricColumn[_], rsd: Double): LongColumn =
    col.elem.map(f.approx_count_distinct(_, rsd)).toDC

  /**
    * Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.approx_count_distinct(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.approx_count_distinct]]
    */
  def aproxCountDistinct(col: DoricColumn[_]): LongColumn =
    col.elem.map(f.approx_count_distinct).toDC

  /**
    * Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.approx_count_distinct(columnName:String,rsd:* org.apache.spark.sql.functions.approx_count_distinct]]
    */
  def aproxCountDistinct(colName: String, rsd: Double): LongColumn =
    aproxCountDistinct(DoricColumn.uncheckedType(colName), rsd)

  /**
    * Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.approx_count_distinct(columnName:String)* org.apache.spark.sql.functions.approx_count_distinct]]
    */
  def aproxCountDistinct(colName: String): LongColumn =
    aproxCountDistinct(DoricColumn.uncheckedType(colName))

  /**
    * Aggregate function: returns the average of the values in a group.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.avg(e:* org.apache.spark.sql.functions.avg]]
    */
  def avg[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.avg).toDC

  /**
    * Aggregate function: returns a list of objects with duplicates.
    *
    * @note The function is non-deterministic because the order of collected results depends
    * on the order of the rows which may be non-deterministic after a shuffle.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.collect_list(e:* org.apache.spark.sql.functions.collect_list]]
    */
  def collectList[T](col: DoricColumn[T]): ArrayColumn[T] =
    col.elem.map(f.collect_list).toDC

  /**
    * Aggregate function: returns a set of objects with duplicate elements eliminated.
    *
    * @note The function is non-deterministic because the order of collected results depends
    * on the order of the rows which may be non-deterministic after a shuffle.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.collect_set(e:* org.apache.spark.sql.functions.collect_set]]
    */
  def collectSet[T](col: DoricColumn[T]): ArrayColumn[T] =
    col.elem.map(f.collect_set).toDC

  /**
    * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.corr(column1:* org.apache.spark.sql.functions.corr]]
    */
  def correlation(col1: DoubleColumn, col2: DoubleColumn): DoubleColumn =
    (col1.elem, col2.elem).mapN(f.corr).toDC

  /**
    * Aggregate function: returns the number of distinct items in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.countDistinct(expr:* org.apache.spark.sql.functions.countDistinct]]
    */
  def countDistinct(expr: DoricColumn[_], exprs: DoricColumn[_]*): LongColumn =
    (expr +: exprs).toList
      .traverse(_.elem)
      .map(x => f.countDistinct(x.head, x.tail: _*))
      .toDC

  /**
    * Aggregate function: returns the number of distinct items in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.countDistinct(columnName:* org.apache.spark.sql.functions.countDistinct]]
    */
  def countDistinct(columnName: CName, columnNames: CName*): LongColumn =
    countDistinct(
      DoricColumn.uncheckedType(columnName.value),
      columnNames.map(x => DoricColumn.uncheckedType(x.value)): _*
    )

  /**
    * Aggregate function: returns the population covariance for two columns.
    *
    * @group Aggregation Double Type
    * @see [[org.apache.spark.sql.functions.covar_pop(column1:* org.apache.spark.sql.functions.covar_pop]]
    */
  def covarPop(col1: DoubleColumn, col2: DoubleColumn): DoubleColumn =
    (col1.elem, col2.elem).mapN(f.covar_pop).toDC

  /**
    * Aggregate function: returns the sample covariance for two columns.
    *
    * @group Aggregation Double Type
    * @see [[org.apache.spark.sql.functions.covar_samp(column1:* org.apache.spark.sql.functions.covar_samp]]
    */
  def covarSamp(col1: DoubleColumn, col2: DoubleColumn): DoubleColumn =
    (col1.elem, col2.elem).mapN(f.covar_samp).toDC

  /**
    * Aggregate function: returns the kurtosis of the values in a group.
    *
    * @group Aggregation Double Type
    * @see [[org.apache.spark.sql.functions.kurtosis(e:* org.apache.spark.sql.functions.kurtosis]]
    */
  def kurtosis(col: DoubleColumn): DoubleColumn =
    col.elem.map(f.kurtosis).toDC

  /**
    * Aggregate function: returns the maximum value of the expression in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.max(e:* org.apache.spark.sql.functions.max]]
    */
  def max[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.max).toDC

  /**
    * Aggregate function: returns the maximum value of the expression in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.min(e:* org.apache.spark.sql.functions.min]]
    */
  def min[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.min).toDC

  /**
    * Aggregate function: returns the maximum value of the expression in a group.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.mean(e:* org.apache.spark.sql.functions.mean]]
    */
  def mean[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.mean).toDC

  /**
    * Aggregate function: returns the skewness of the values in a group.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.skewness(e:* org.apache.spark.sql.functions.skewness]]
    */
  def skewness[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.skewness).toDC

  /**
    * Aggregate function: alias for `stddev_samp`.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.stddev(e:* org.apache.spark.sql.functions.stddev]]
    */
  def stdDev[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.stddev).toDC

  /**
    * Aggregate function: returns the sample standard deviation of the expression in a group.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.stddev_samp(e:* org.apache.spark.sql.functions.stddev_samp]]
    */
  def stdDevSamp[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.stddev_samp).toDC

  /**
    * Aggregate function: returns the population standard deviation of the expression in a group.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.stddev_pop(e:* org.apache.spark.sql.functions.stddev_pop]]
    */
  def stdDevPop[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.stddev_pop).toDC

  /**
    * Aggregate function: returns the sum of distinct values in the expression.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.sumDistinct(e:* org.apache.spark.sql.functions.sumDistinct]]
    */
  def sumDistinct[T](col: DoricColumn[T])(implicit
      nt: NumericType[T]
  ): DoricColumn[nt.Sum] =
    col.elem
      .map(e =>
        new Column(Sum(e.expr).toAggregateExpression(isDistinct = true))
      )
      .toDC

  /**
    * Aggregate function: alias for `var_samp`.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.variance(e:* org.apache.spark.sql.functions.variance]]
    */
  def variance[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.variance).toDC

  /**
    * Aggregate function: returns the unbiased variance of the values in a group.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.var_samp(e:* org.apache.spark.sql.functions.var_samp]]
    */
  def varSamp[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.var_samp).toDC

  /**
    * Aggregate function: returns the population variance of the values in a group.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.var_pop(e:* org.apache.spark.sql.functions.var_pop]]
    */
  def varPop[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.var_pop).toDC

  /**
    * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated
    * or not, returns 1 for aggregated or 0 for not aggregated in the result set.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.grouping(e:* org.apache.spark.sql.functions.grouping]]
    */
  def grouping(col: DoricColumn[_]): ByteColumn =
    col.elem.map(f.grouping).toDC

  /**
    * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated
    * or not, returns 1 for aggregated or 0 for not aggregated in the result set.
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.grouping(columnName:* org.apache.spark.sql.functions.grouping]]
    */
  def grouping(columnName: CName): ByteColumn =
    DoricColumn.uncheckedType(columnName.value).elem.map(f.grouping).toDC

  /**
    * Aggregate function: returns the level of grouping, equals to
    *
    * @example {{{
    *   (grouping(c1) <<; (n-1)) + (grouping(c2) <<; (n-2)) + ... + grouping(cn)
    * }}}
    *
    * @note The list of columns should match with grouping columns exactly, or empty (means all the
    * grouping columns).
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.grouping_id(cols:* org.apache.spark.sql.functions.grouping_id]]
    */
  def groupingId(col: DoricColumn[_], cols: DoricColumn[_]*): LongColumn =
    (col +: cols).map(_.elem).toList.sequence.map(f.grouping_id(_: _*)).toDC

  /**
    * Aggregate function: returns the level of grouping, equals to
    *
    * @example {{{
    *   (grouping(c1) <<; (n-1)) + (grouping(c2) <<; (n-2)) + ... + grouping(cn)
    * }}}
    *
    * @note The list of columns should match with grouping columns exactly, or empty (means all the
    * grouping columns).
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.grouping_id(cols:* org.apache.spark.sql.functions.grouping_id]]
    */
  def groupingId(colName: CName, colNames: CName*): LongColumn =
    Doric(f.grouping_id(colName.value, colNames.map(_.value): _*)).toDC

}
