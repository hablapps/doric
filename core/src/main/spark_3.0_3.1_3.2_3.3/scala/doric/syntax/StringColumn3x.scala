package doric
package syntax

import cats.implicits._
import org.apache.spark.sql.catalyst.expressions.StringSplit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, functions => f}

import scala.jdk.CollectionConverters._

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

    /**
      * Parses a CSV string and infers its schema in DDL format.
      * @throws org.apache.spark.sql.AnalysisException if it is not a foldable string expression or null
      * @group String Type
      * @see [[org.apache.spark.sql.functions.schema_of_csv(csv:org\.apache\.spark\.sql\.Column,options:* org.apache.spark.sql.functions.schema_of_csv]]
      */
    def schemaOfCsv(options: Map[String, String] = Map.empty): StringColumn = {
      s.elem.map(x => f.schema_of_csv(x, options.asJava)).toDC
    }

    /**
      * Parses a JSON string and infers its schema in DDL format.
      * @throws org.apache.spark.sql.AnalysisException if it is not a foldable string expression or null
      *
      * @group String Type
      * @see [[org.apache.spark.sql.functions.schema_of_json(json:org\.apache\.spark\.sql\.Column,options:* org.apache.spark.sql.functions.schema_of_json]]
      */
    def schemaOfJson(options: Map[String, String] = Map.empty): StringColumn = {
      s.elem.map(x => f.schema_of_json(x, options.asJava)).toDC
    }

    /**
      * Parses a column containing a CSV string into a StructType with the specified schema.
      *
      * @note Returns null, in the case of an unparseable string.
      * @group String Type
      * @see [[org.apache.spark.sql.functions.from_csv(e:org\.apache\.spark\.sql\.Column,schema:org\.apache\.spark\.sql\.Column,options:* org.apache.spark.sql.functions.from_csv]]
      */
    def fromCsvString(
        schema: StringColumn,
        options: Map[String, String] = Map.empty
    ): RowColumn =
      (s.elem, schema.elem)
        .mapN((x, y) => f.from_csv(x, y, options.asJava))
        .toDC

    /**
      * Parses a column containing a CSV string into a StructType with the specified schema.
      *
      * @note Returns null, in the case of an unparseable string.
      * @group String Type
      * @see [[org.apache.spark.sql.functions.from_csv(e:org\.apache\.spark\.sql\.Column,schema:org\.apache\.spark\.sql\.types\.StructType,options:* org.apache.spark.sql.functions.from_csv]]
      * @todo here we have an error because of same function name
      */
    def fromCsvStruct(
        schema: StructType,
        options: Map[String, String] = Map.empty
    ): RowColumn =
      s.elem.map(x => f.from_csv(x, schema, options)).toDC
  }
}
