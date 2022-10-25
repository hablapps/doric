package doric
package syntax

import cats.implicits._

import org.apache.spark.sql.{Column, functions => f}
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

    /**
      * Parses a JSON string and infers its schema in DDL format.
      * @throws org.apache.spark.sql.AnalysisException if it is not a foldable string expression or null
      *
      * @group String Type
      * @see [[org.apache.spark.sql.functions.schema_of_json(json:org\.apache\.spark\.sql\.Column):* org.apache.spark.sql.functions.schema_of_json]]
      */
    def schemaOfJson(): StringColumn = s.elem.map(f.schema_of_json).toDC
  }
}
