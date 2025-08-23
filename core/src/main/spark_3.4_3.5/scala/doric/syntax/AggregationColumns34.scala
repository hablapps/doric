package doric
package syntax

import doric.types.NumericType
import org.apache.spark.sql.{functions => f}

trait AggregationColumns34 {

  /**
    * Aggregate function: returns the most frequent value in a group
    *
    * @group Aggregation Any Type
    * @see [[org.apache.spark.sql.functions.mode]]
    */
  def mode[T](col: DoricColumn[T]): DoricColumn[T] =
    col.elem.map(f.mode).toDC

  /**
    * Aggregate function: returns the median of the values in a group.
    *
    * @group Aggregation Numeric Type
    * @see [[org.apache.spark.sql.functions.median]]
    */
  def median[T: NumericType](col: DoricColumn[T]): DoubleColumn =
    col.elem.map(f.median).toDC
}
