package doric
package syntax

import cats.implicits.toTraverseOps

import org.apache.spark.sql.{functions => f}

trait StringColumns {

  /**
    * Concatenate string columns to form a single one
    * @param cols the String DoricColumns to concatenate
    * @return a reference of a single DoricColumn with all strings concatenated. If at least one is null will return null.
    */
  def concat(cols: DoricColumn[String]*): DoricColumn[String] =
    cols.map(_.elem).toList.sequence.map(f.concat(_: _*)).toDC
}
