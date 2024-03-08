package doric
package syntax

import scala.jdk.CollectionConverters._

import doric.types.SparkType

import org.apache.spark.sql.{Row, functions => f}

private[syntax] trait DStructs3x {

  implicit class DStructOps3x[T](private val col: DoricColumn[T])(implicit
      st: SparkType.Custom[T, Row]
  ) {

    /**
      * Converts a column containing a StructType into a CSV string with the specified schema.
      * @throws java.lang.IllegalArgumentException in the case of an unsupported type.
      *
      * @group Struct Type
      * @see [[org.apache.spark.sql.functions.to_csv(e:org\.apache\.spark\.sql\.Column,options:* org.apache.spark.sql.functions.to_csv]]
      */
    def toCsv(options: Map[String, String] = Map.empty): StringColumn =
      col.elem.map(x => f.to_csv(x, options.asJava)).toDC
  }

}
