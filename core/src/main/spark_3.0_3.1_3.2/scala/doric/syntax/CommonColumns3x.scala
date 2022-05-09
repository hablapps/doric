package doric
package syntax

import cats.implicits._

import org.apache.spark.sql.{functions => f}

trait CommonColumns3x {

  /**
    * Calculates the hash code of given columns using the 64-bit
    * variant of the xxHash algorithm, and returns the result as a long column.
    *
    * @group All Types
    * @see [[org.apache.spark.sql.functions.xxhash64]]
    */
  def xxhash64(cols: DoricColumn[_]*): LongColumn =
    cols.map(_.elem).toList.sequence.map(f.xxhash64(_: _*)).toDC
}
