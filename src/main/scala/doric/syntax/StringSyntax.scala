package doric
package syntax

import cats.implicits.toTraverseOps

import org.apache.spark.sql.{functions => f}

trait StringSyntax {
  def concat(cols: DoricColumn[String]*): DoricColumn[String] =
    cols.map(_.elem).toList.sequence.map(f.concat(_: _*)).toDC
}
