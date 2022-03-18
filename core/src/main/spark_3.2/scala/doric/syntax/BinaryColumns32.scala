package doric
package syntax

import cats.implicits.catsSyntaxTuple2Semigroupal
import doric.types.{BinaryType, SparkType}

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.StringDecode

private[syntax] trait BinaryColumns32 {

  implicit class BinaryOperationsSyntax32[T: BinaryType: SparkType](
      column: DoricColumn[T]
  ) {

    /**
      * Computes the first argument into a string from a binary using the provided character set
      * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
      * If either argument is null, the result will also be null.
      *
      * @group Binary Type
      * @see [[org.apache.spark.sql.functions.decode]]
      */
    def decode(charset: StringColumn): StringColumn =
      (column.elem, charset.elem)
        .mapN((col, char) => {
          new Column(StringDecode(col.expr, char.expr))
        })
        .toDC
  }

}
