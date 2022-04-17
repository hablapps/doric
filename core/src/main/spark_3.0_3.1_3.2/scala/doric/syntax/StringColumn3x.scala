package doric
package syntax

import cats.implicits._

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions.StringSplit

trait StringColumn3x {
  implicit class StringOperationsSyntax3x(s: DoricColumn[String]) {

    /**
      * Overlay the specified portion of `src` with `replace`, starting from
      * byte position `pos` of `src` and proceeding for `len` bytes.
      *
      * @group String Type
      * @see [[org.apache.spark.sql.functions.overlay(src:org\.apache\.spark\.sql\.Column,replace:org\.apache\.spark\.sql\.Column,pos:org\.apache\.spark\.sql\.Column,len:org\.apache\.spark\.sql\.Column):* org.apache.spark.sql.functions.overlay]]
      */
    def overlay(
        replace: StringColumn,
        pos: IntegerColumn,
        len: IntegerColumn = (-1).lit
    ): StringColumn =
      (s.elem, replace.elem, pos.elem, len.elem).mapN(f.overlay).toDC

    /**
      * Splits str around matches of the given pattern.
      *
      * @group String Type
      * @param pattern
      * a string representing a regular expression. The regex string should be
      * a Java regular expression.
      * @param limit
      * an integer expression which controls the number of times the regex is applied.
      *     - __limit greater than 0__: The resulting array's length
      *       will not be more than limit, and the resulting array's last entry will
      *       contain all input beyond the last matched regex.
      *     - __limit less than or equal to 0__: `regex` will be applied as many times as possible,
      *       and the resulting array can be of any size.
      * @see org.apache.spark.sql.functions.split
      * @todo scaladoc link (issue #135)
      */
    def split(
        pattern: StringColumn,
        limit: IntegerColumn = (-1).lit
    ): ArrayColumn[String] =
      (s.elem, pattern.elem, limit.elem)
        .mapN((str, p, l) => new Column(StringSplit(str.expr, p.expr, l.expr)))
        .toDC
  }
}
