package doric
package syntax

import doric.sem.Location
import doric.types.{NumericType, SparkType}

import org.apache.spark.sql.{functions => f}

trait AggregationColumns {

  def sum[T: NumericType](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.sum).toDC

  def sum[T: NumericType: SparkType](colName: String)(implicit
      location: Location
  ): DoricColumn[T] =
    sum[T](col[T](colName))

  def count(col: DoricColumn[_]): LongColumn =
    col.elem.map(f.count).toDC

  def count(colName: String)(implicit location: Location): LongColumn =
    count(DoricColumn.uncheckedType(f.col(colName)))

  def first[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.first).toDC

  def first[T](col: DoricColumn[T], ignoreNulls: Boolean): DoricColumn[T] =
    col.elem.map(f.first(_, ignoreNulls)).toDC

  def first[T: SparkType](colName: String): DoricColumn[T] =
    first[T](col[T](colName))

  def first[T: SparkType](
      colName: String,
      ignoreNulls: Boolean
  ): DoricColumn[T] =
    first[T](col[T](colName), ignoreNulls)

  def last[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.last).toDC

  def last[T](col: DoricColumn[T], ignoreNulls: Boolean): DoricColumn[T] =
    col.elem.map(f.last(_, ignoreNulls)).toDC

  def last[T: SparkType](colName: String): DoricColumn[T] =
    last[T](col[T](colName))

  def last[T: SparkType](
      colName: String,
      ignoreNulls: Boolean
  ): DoricColumn[T] =
    last[T](col[T](colName), ignoreNulls)
}
