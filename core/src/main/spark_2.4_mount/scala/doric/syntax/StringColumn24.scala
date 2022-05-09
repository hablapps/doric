package doric
package syntax

import cats.implicits._

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.StringSplit

trait StringColumn24 {
  implicit class StringOperationsSyntax24(s: DoricColumn[String]) {

    /**
      * Splits str around matches of the given pattern.
      *
      * @group String Type
      * @param pattern
      * a string representing a regular expression. The regex string should be
      * a Java regular expression.
      * @see org.apache.spark.sql.functions.split
      * @todo scaladoc link (issue #135)
      */
    def split(
        pattern: StringColumn
    ): ArrayColumn[String] =
      (s.elem, pattern.elem)
        .mapN((str, p) => new Column(StringSplit(str.expr, p.expr)))
        .toDC
  }
}
